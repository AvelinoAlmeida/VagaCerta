'use strict';
// marcacoes: orquestra a marcação de uma consulta.
//   POST   /marcacoes  {utente, vagaId} + Idempotency-Key   valida o utente, reserva na agenda, publica marcacao.criada
//   GET    /marcacoes/:id
//   DELETE /marcacoes/:id                                     desmarca: liberta a vaga na agenda
//   GET    /audit                                             contagens e vagas com mais de uma marcação (deve ser 0)
//
// Idempotência: a chave fica gravada com o marcacaoId antes de se chamar a agenda. Um retry do cliente com a mesma
// chave recebe a mesma resposta; se a primeira tentativa ficou a meio (pendente), continua com o mesmo marcacaoId.
// Confirmação (outbox): a marcação fica com notificada=false até o RabbitMQ confirmar a mensagem. Se o RabbitMQ
// estiver em baixo a marcação responde 201 na mesma, e um ciclo de fundo republica as pendentes quando ele voltar.
const crypto = require('node:crypto');
const express = require('express');
const { Pool } = require('pg');
const { str, num, makeLogger, reply, retry, createService, client } = require('./lib');
const { QUEUE, amqpConnection } = require('./amqp');
const { clientes } = require('./clientes');

const log = makeLogger('marcacoes');
const config = {
  port: num('PORT', 3002),
  databaseUrl: str('DATABASE_URL', 'postgres://app:app@localhost:5432/marcacoes'),
  rabbitUrl: str('RABBITMQ_URL', 'amqp://app:app@localhost:5672'),
  agendaUrl: str('AGENDA_URL', 'http://localhost:3001'),
  utentesUrl: str('UTENTES_URL', 'http://localhost:3003'),
  timeoutMs: num('TIMEOUT_MS', 300),
  retries: num('RETRIES', 2),
  backoffMs: num('BACKOFF_MS', 50),
  outboxMs: num('OUTBOX_INTERVAL_MS', 2000),
  pgPoolMax: num('PG_POOL_MAX', 10),
};

const pool = new Pool({ connectionString: config.databaseUrl, max: config.pgPoolMax });
pool.on('error', (err) => log.warn('postgres pool error', { err: err.message }));

const marcacoesTotal = new client.Counter({ name: 'marcacoes_total', help: 'Pedidos de marcação por resultado', labelNames: ['resultado'] });
const retriesTotal = new client.Counter({ name: 'marcacoes_retries_total', help: 'Retries para outros serviços', labelNames: ['destino'] });
const publicadas = new client.Counter({ name: 'marcacoes_publicadas_total', help: 'Mensagens marcacao.criada confirmadas pelo RabbitMQ' });
const porNotificar = new client.Gauge({ name: 'marcacoes_por_notificar', help: 'Marcações confirmadas ainda sem mensagem publicada' });
for (const resultado of ['confirmada', 'repetida', 'ocupada', 'utente_inexistente', 'vaga_inexistente', 'indisponivel']) marcacoesTotal.inc({ resultado }, 0);

const api = clientes({
  ...config,
  onRetry: (destino) => (tentativa, err) => {
    retriesTotal.inc({ destino });
    log.warn('retry', { destino, tentativa, timeout: err.timeout, err: err.message });
  },
});

const broker = amqpConnection({ url: config.rabbitUrl, log, confirm: true });

// Publica uma marcação e marca-a como notificada só depois de o broker confirmar (publisher confirms).
async function publicar(m) {
  const ch = broker.channel();
  if (!ch) return false;
  const msg = { marcacaoId: m.id, utente: m.utente, vagaId: m.vaga_id, criadaEm: m.criada_em };
  try {
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('sem confirmação do broker (2 s)')), 2000);
      ch.sendToQueue(QUEUE, Buffer.from(JSON.stringify(msg)), { persistent: true, messageId: m.id, contentType: 'application/json' }, (err) => {
        clearTimeout(t);
        if (err) reject(err); else resolve();
      });
    });
  } catch (err) {
    log.warn('publicação falhou, fica no outbox', { marcacaoId: m.id, err: err.message });
    return false;
  }
  await pool.query('UPDATE marcacoes SET notificada = true WHERE id = $1', [m.id]);
  publicadas.inc();
  return true;
}

// Ciclo do outbox: republica as confirmadas que ainda não chegaram ao RabbitMQ. Duplicados são inofensivos
// (o consumidor deduplica pelo marcacaoId), por isso várias réplicas podem correr isto ao mesmo tempo.
let outboxTimer = null;
async function cicloOutbox() {
  try {
    const r = await pool.query(`SELECT * FROM marcacoes WHERE estado = 'confirmada' AND NOT notificada ORDER BY criada_em LIMIT 200`);
    porNotificar.set(r.rowCount);
    if (r.rowCount && broker.isUp()) {
      let ok = 0;
      for (const m of r.rows) if (await publicar(m)) ok++;
      log.info('outbox', { pendentes: r.rowCount, publicadas: ok });
    }
  } catch (err) {
    log.warn('outbox error', { err: err.message });
  }
  outboxTimer = setTimeout(cicloOutbox, config.outboxMs);
}

const svc = createService({
  name: 'marcacoes',
  port: config.port,
  log,
  init: async () => {
    await retry(async () => {
      const c = await pool.connect();
      try {
        await c.query('BEGIN');
        await c.query('SELECT pg_advisory_xact_lock(8002)');
        await c.query(`CREATE TABLE IF NOT EXISTS marcacoes (
          id              text PRIMARY KEY,
          idempotency_key text NOT NULL UNIQUE,
          utente          text NOT NULL,
          vaga_id         text NOT NULL,
          estado          text NOT NULL CHECK (estado IN ('pendente', 'confirmada', 'rejeitada', 'cancelada')),
          motivo          text,
          notificada      boolean NOT NULL DEFAULT false,
          criada_em       timestamptz NOT NULL DEFAULT now(),
          atualizada_em   timestamptz NOT NULL DEFAULT now())`);
        await c.query('CREATE INDEX IF NOT EXISTS marcacoes_outbox ON marcacoes (criada_em) WHERE estado = \'confirmada\' AND NOT notificada');
        await c.query('COMMIT');
      } catch (err) {
        await c.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        c.release();
      }
    }, { what: 'postgres', log });
    broker.connect(); // não esperamos: sem RabbitMQ as marcações continuam (E5)
    cicloOutbox();
    log.info('config', { timeoutMs: config.timeoutMs, retries: config.retries, backoffMs: config.backoffMs });
  },
  checks: { postgres: () => pool.query('SELECT 1') }, // o RabbitMQ fica de fora de propósito: não deve tirar o serviço do ar
  shutdown: async () => {
    clearTimeout(outboxTimer);
    await broker.close();
    await pool.end();
  },
});

const marcacaoJson = (m) => ({
  id: m.id, utente: m.utente, vagaId: m.vaga_id, estado: m.estado, notificada: m.notificada, criadaEm: m.criada_em,
  ...(m.motivo ? { motivo: m.motivo } : {}),
});
const STATUS_REJEICAO = { vaga_ocupada: 409, vaga_inexistente: 404, utente_inexistente: 422 };

async function rejeitar(m, motivo) {
  const r = await pool.query(`UPDATE marcacoes SET estado = 'rejeitada', motivo = $2, atualizada_em = now() WHERE id = $1 RETURNING *`, [m.id, motivo]);
  marcacoesTotal.inc({ resultado: motivo === 'vaga_ocupada' ? 'ocupada' : motivo });
  return r.rows[0];
}

svc.start((app) => {
  app.use(express.json());

  app.post('/marcacoes', async (req, res) => {
    const chave = req.get('idempotency-key');
    const { utente, vagaId } = req.body || {};
    if (!chave) return reply(res, 400, { error: 'falta o cabeçalho Idempotency-Key' });
    if (typeof utente !== 'string' || typeof vagaId !== 'string' || !utente || !vagaId) {
      return reply(res, 400, { error: 'esperado {utente: string, vagaId: string}' });
    }
    const requestId = res.get('x-request-id');

    // 1. Grava a chave (ou encontra a que já existe).
    let m = (await pool.query(
      `INSERT INTO marcacoes (id, idempotency_key, utente, vaga_id, estado) VALUES ($1, $2, $3, $4, 'pendente')
       ON CONFLICT (idempotency_key) DO NOTHING RETURNING *`, [crypto.randomUUID(), chave, utente, vagaId])).rows[0];
    if (!m) {
      m = (await pool.query('SELECT * FROM marcacoes WHERE idempotency_key = $1', [chave])).rows[0];
      if (m.utente !== utente || m.vaga_id !== vagaId) return reply(res, 422, { error: 'chave_reutilizada', detalhe: 'a mesma chave foi usada com outro pedido' });
      if (m.estado !== 'pendente') {
        marcacoesTotal.inc({ resultado: 'repetida' });
        res.set('idempotent-replayed', 'true');
        const status = m.estado === 'rejeitada' ? STATUS_REJEICAO[m.motivo] : m.estado === 'confirmada' ? 201 : 200;
        return reply(res, status, marcacaoJson(m));
      }
      res.set('idempotent-replayed', 'true'); // ficou a meio: continua com o mesmo marcacaoId
    }

    try {
      // 2. Valida o utente.
      const u = await api.utente(utente, requestId);
      if (u.status === 404) return reply(res, 422, marcacaoJson(await rejeitar(m, 'utente_inexistente')));
      if (u.status !== 200) return reply(res, 400, { error: 'utente_invalido', detalhe: u.body.error });

      // 3. Reserva a vaga (o UPDATE condicional na agenda decide quem fica com ela).
      const r = await api.reservar(vagaId, utente, m.id, requestId);
      if (r.status === 409) return reply(res, 409, marcacaoJson(await rejeitar(m, 'vaga_ocupada')));
      if (r.status === 404) return reply(res, 404, marcacaoJson(await rejeitar(m, 'vaga_inexistente')));
      if (r.status !== 201 && r.status !== 200) return reply(res, 502, { error: 'resposta_inesperada_da_agenda', status: r.status });
    } catch (err) {
      // Utentes ou agenda não responderam depois dos retries. A marcação fica pendente: o cliente pode repetir com a mesma chave.
      marcacoesTotal.inc({ resultado: 'indisponivel' });
      log.warn('marcação por concluir', { marcacaoId: m.id, err: err.message, tentativas: err.tentativas });
      return reply(res, 503, { error: 'servico_indisponivel', marcacaoId: m.id, tentativas: err.tentativas });
    }

    m = (await pool.query(`UPDATE marcacoes SET estado = 'confirmada', atualizada_em = now() WHERE id = $1 RETURNING *`, [m.id])).rows[0];
    marcacoesTotal.inc({ resultado: 'confirmada' });

    // 4. Confirmação assíncrona: tenta já, e se falhar o outbox trata disso.
    if (await publicar(m)) m.notificada = true;
    reply(res, 201, marcacaoJson(m));
  });

  app.get('/marcacoes/:id', async (req, res) => {
    const r = await pool.query('SELECT * FROM marcacoes WHERE id = $1', [req.params.id]);
    if (!r.rowCount) return reply(res, 404, { error: 'marcacao_inexistente' });
    reply(res, 200, marcacaoJson(r.rows[0]));
  });

  app.delete('/marcacoes/:id', async (req, res) => {
    const r = await pool.query('SELECT * FROM marcacoes WHERE id = $1', [req.params.id]);
    if (!r.rowCount) return reply(res, 404, { error: 'marcacao_inexistente' });
    const m = r.rows[0];
    if (m.estado === 'cancelada') return reply(res, 200, marcacaoJson(m));
    if (m.estado !== 'confirmada') return reply(res, 409, { error: 'marcacao_nao_confirmada', estado: m.estado });
    try {
      // Primeiro liberta a vaga; se a agenda falhar, a marcação continua confirmada e o pedido pode ser repetido.
      await api.libertar(m.vaga_id, m.id, res.get('x-request-id'));
    } catch (err) {
      return reply(res, 503, { error: 'servico_indisponivel', detalhe: err.message });
    }
    const c = await pool.query(`UPDATE marcacoes SET estado = 'cancelada', atualizada_em = now() WHERE id = $1 RETURNING *`, [m.id]);
    reply(res, 200, marcacaoJson(c.rows[0]));
  });

  app.get('/audit', async (req, res) => {
    const totais = await pool.query(
      `SELECT count(*)::int AS pedidos,
              count(*) FILTER (WHERE estado = 'confirmada')::int AS confirmadas,
              count(*) FILTER (WHERE estado = 'rejeitada')::int AS rejeitadas,
              count(*) FILTER (WHERE estado = 'pendente')::int AS pendentes,
              count(*) FILTER (WHERE estado = 'cancelada')::int AS canceladas,
              count(*) FILTER (WHERE estado = 'confirmada' AND NOT notificada)::int AS por_notificar
         FROM marcacoes`);
    // RNF1: nenhuma vaga com mais de uma marcação confirmada.
    const dup = await pool.query(
      `SELECT vaga_id, count(*)::int AS marcacoes FROM marcacoes WHERE estado = 'confirmada'
        GROUP BY vaga_id HAVING count(*) > 1 ORDER BY 2 DESC LIMIT 20`);
    reply(res, 200, { ...totais.rows[0], vagas_duplicadas: dup.rowCount, duplicados: dup.rows });
  });
});
