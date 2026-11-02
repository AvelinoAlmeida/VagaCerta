'use strict';
// notificacoes: consome a fila marcacao.criada e "envia" o SMS de confirmação.
//   RabbitMQ: fila durável marcacao.criada, ack manual, prefetch
//   GET /sms/:marcacaoId   o SMS enviado para uma marcação (404 se ainda não foi)
//   GET /stats             mensagens recebidas vs. SMS distintos enviados
// "Enviar" = esperar SMS_DELAY_MS, talvez falhar (SMS_FAIL_RATE), e registar no Postgres com ON CONFLICT DO NOTHING:
// uma mensagem repetida (outbox, redelivery) não gera um segundo SMS (RNF4: pelo menos uma vez, sem duplicar).
const { Pool } = require('pg');
const { str, num, sleep, makeLogger, reply, retry, createService, client } = require('./lib');
const { QUEUE, DLQ, amqpConnection } = require('./amqp');

const log = makeLogger('notificacoes');
const config = {
  port: num('PORT', 3004),
  databaseUrl: str('DATABASE_URL', 'postgres://app:app@localhost:5432/notificacoes'),
  rabbitUrl: str('RABBITMQ_URL', 'amqp://app:app@localhost:5672'),
  prefetch: num('PREFETCH', 10),
  delayMs: num('SMS_DELAY_MS', 0), // operadora de SMS lenta
  failRate: num('SMS_FAIL_RATE', 0), // 0..1
  pgPoolMax: num('PG_POOL_MAX', 10),
};

const pool = new Pool({ connectionString: config.databaseUrl, max: config.pgPoolMax });
pool.on('error', (err) => log.warn('postgres pool error', { err: err.message }));

const recebidas = new client.Counter({ name: 'notificacoes_mensagens_total', help: 'Mensagens recebidas (incluindo repetidas)' });
const enviados = new client.Counter({ name: 'notificacoes_sms_enviados_total', help: 'SMS distintos enviados' });
const repetidas = new client.Counter({ name: 'notificacoes_repetidas_total', help: 'Mensagens de marcações já notificadas (ignoradas)' });
const falhas = new client.Counter({ name: 'notificacoes_falhas_total', help: 'Tentativas de envio falhadas' });

async function enviarSms(m) {
  await sleep(config.delayMs);
  if (Math.random() < config.failRate) {
    falhas.inc();
    throw new Error('falha simulada da operadora');
  }
  const texto = `Consulta marcada: vaga ${m.vagaId}. Ref ${m.marcacaoId.slice(0, 8)}.`;
  const r = await pool.query(
    'INSERT INTO sms_enviados (marcacao_id, utente, vaga_id, texto) VALUES ($1, $2, $3, $4) ON CONFLICT (marcacao_id) DO NOTHING',
    [m.marcacaoId, m.utente, m.vagaId, texto]);
  if (r.rowCount) {
    enviados.inc();
    log.info('sms enviado', { marcacaoId: m.marcacaoId, vagaId: m.vagaId });
  } else {
    repetidas.inc();
  }
}

async function tratar(ch, msg) {
  if (!msg) return; // consumidor cancelado pelo broker
  recebidas.inc();
  let m;
  try {
    m = JSON.parse(msg.content.toString());
    if (!m.marcacaoId) throw new Error('falta marcacaoId');
  } catch (err) {
    log.error('mensagem ilegível, vai para a DLQ', { err: err.message });
    return ch.nack(msg, false, false);
  }
  try {
    await enviarSms(m);
    ch.ack(msg); // ack só depois de registado: se o processo morrer antes, o broker volta a entregar
  } catch (err) {
    // Primeira falha: volta para a fila. Segunda (já era uma reentrega): vai para a DLQ.
    log.warn('envio falhou', { marcacaoId: m.marcacaoId, reentrega: msg.fields.redelivered, err: err.message });
    ch.nack(msg, false, !msg.fields.redelivered);
  }
}

const broker = amqpConnection({
  url: config.rabbitUrl,
  log,
  onChannel: async (ch) => {
    await ch.prefetch(config.prefetch);
    await ch.consume(QUEUE, (msg) => tratar(ch, msg).catch((err) => log.warn('erro a tratar mensagem', { err: err.message })), { noAck: false });
    log.info('a consumir', { queue: QUEUE, dlq: DLQ, prefetch: config.prefetch });
  },
});

const svc = createService({
  name: 'notificacoes',
  port: config.port,
  log,
  init: async () => {
    await retry(async () => {
      const c = await pool.connect();
      try {
        await c.query('BEGIN');
        await c.query('SELECT pg_advisory_xact_lock(8004)');
        await c.query(`CREATE TABLE IF NOT EXISTS sms_enviados (
          marcacao_id text PRIMARY KEY,
          utente      text NOT NULL,
          vaga_id     text NOT NULL,
          texto       text NOT NULL,
          enviado_em  timestamptz NOT NULL DEFAULT now())`);
        await c.query('COMMIT');
      } catch (err) {
        await c.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        c.release();
      }
    }, { what: 'postgres', log });
    await broker.connect();
    log.info('config', { prefetch: config.prefetch, delayMs: config.delayMs, failRate: config.failRate });
  },
  checks: {
    postgres: () => pool.query('SELECT 1'),
    rabbitmq: async () => { if (!broker.isUp()) throw new Error('sem ligação'); },
  },
  shutdown: async () => {
    await broker.close(); // deixa de consumir; as mensagens sem ack voltam para a fila
    await pool.end();
  },
});

svc.start((app) => {
  app.get('/sms/:marcacaoId', async (req, res) => {
    const r = await pool.query('SELECT marcacao_id, vaga_id, texto, enviado_em FROM sms_enviados WHERE marcacao_id = $1', [req.params.marcacaoId]);
    if (!r.rowCount) return reply(res, 404, { error: 'sms_nao_enviado' });
    const s = r.rows[0];
    reply(res, 200, { marcacaoId: s.marcacao_id, vagaId: s.vaga_id, texto: s.texto, enviadoEm: s.enviado_em });
  });

  app.get('/stats', async (req, res) => {
    const r = await pool.query('SELECT count(*)::int AS sms_enviados FROM sms_enviados');
    reply(res, 200, { ...r.rows[0] });
  });
});
