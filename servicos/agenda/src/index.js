'use strict';
// agenda: as vagas de consulta. Única fonte de verdade sobre quem tem cada vaga.
//   GET  /agenda?especialidade=CARD&dia=2026-12-02   vagas livres (lidas em READ_DATABASE_URL: a réplica, quando existir)
//   GET  /vagas/:id                                    estado de uma vaga (primário)
//   POST /vagas/:id/reservar  {utente, marcacaoId}     livre -> marcada, com UPDATE condicional (0 linhas = 409)
//   POST /vagas/:id/libertar  {marcacaoId}             marcada -> livre, só pela marcação que a tem
//   GET  /audit                                        contagens por estado (para as experiências)
// Reservar e libertar são idempotentes pelo marcacaoId: um retry do mesmo pedido recebe a mesma resposta.
const express = require('express');
const { Pool } = require('pg');
const { str, num, sleep, makeLogger, reply, retry, createService, client } = require('./lib');

const log = makeLogger('agenda');
const config = {
  port: num('PORT', 3001),
  databaseUrl: str('DATABASE_URL', 'postgres://app:app@localhost:5432/agenda'),
  readDatabaseUrl: str('READ_DATABASE_URL', ''), // vazio = ler do primário
  seedDias: str('SEED_DIAS', '2026-12-02').split(',').map((d) => d.trim()).filter(Boolean),
  delayMs: num('RESERVA_DELAY_MS', 0), // atraso artificial na reserva (experiência E2)
  pgPoolMax: num('PG_POOL_MAX', 10),
};

const ESPECIALIDADES = { CARD: 'Cardiologia', MGF: 'Medicina Geral e Familiar', ORTO: 'Ortopedia', OFT: 'Oftalmologia' };
const HORAS = []; // 08:00 a 11:45, de 15 em 15 minutos: 16 vagas por especialidade e dia
for (let m = 8 * 60; m < 12 * 60; m += 15) HORAS.push(`${String(Math.floor(m / 60)).padStart(2, '0')}${String(m % 60).padStart(2, '0')}`);

const pool = new Pool({ connectionString: config.databaseUrl, max: config.pgPoolMax });
const readPool = config.readDatabaseUrl ? new Pool({ connectionString: config.readDatabaseUrl, max: config.pgPoolMax }) : pool;
for (const p of new Set([pool, readPool])) p.on('error', (err) => log.warn('postgres pool error', { err: err.message }));

const reservas = new client.Counter({ name: 'agenda_reservas_total', help: 'Pedidos de reserva por resultado', labelNames: ['resultado'] });
for (const resultado of ['marcada', 'repetida', 'ocupada', 'inexistente']) reservas.inc({ resultado }, 0);

const vagaId = (dia, esp, hora) => `V-${dia}-${esp}-${hora}`;

async function seed(c) {
  for (const dia of config.seedDias) {
    const ids = [];
    const esps = [];
    const horas = [];
    for (const esp of Object.keys(ESPECIALIDADES)) {
      for (const hora of HORAS) { ids.push(vagaId(dia, esp, hora)); esps.push(esp); horas.push(`${hora.slice(0, 2)}:${hora.slice(2)}`); }
    }
    const r = await c.query(
      `INSERT INTO vagas (id, especialidade, dia, hora)
       SELECT * FROM unnest($1::text[], $2::text[], array_fill($3::date, ARRAY[cardinality($1::text[])]), $4::time[])
       ON CONFLICT (id) DO NOTHING`, [ids, esps, dia, horas]);
    if (r.rowCount) log.info('vagas criadas', { dia, vagas: r.rowCount });
  }
}

const svc = createService({
  name: 'agenda',
  port: config.port,
  log,
  init: async () => {
    await retry(async () => {
      const c = await pool.connect();
      try {
        await c.query('BEGIN');
        await c.query('SELECT pg_advisory_xact_lock(8001)'); // várias réplicas a arrancar: só uma cria o esquema de cada vez
        await c.query(`CREATE TABLE IF NOT EXISTS vagas (
          id            text PRIMARY KEY,
          especialidade text NOT NULL,
          dia           date NOT NULL,
          hora          time NOT NULL,
          estado        text NOT NULL DEFAULT 'livre' CHECK (estado IN ('livre', 'marcada', 'cancelada')),
          utente        text,
          marcacao_id   text,
          atualizada_em timestamptz NOT NULL DEFAULT now())`);
        await c.query('CREATE INDEX IF NOT EXISTS vagas_livres ON vagas (especialidade, dia) WHERE estado = \'livre\'');
        await seed(c);
        await c.query('COMMIT');
      } catch (err) {
        await c.query('ROLLBACK').catch(() => {});
        throw err;
      } finally {
        c.release();
      }
    }, { what: 'postgres', log });
    log.info('config', { seedDias: config.seedDias, replica: Boolean(config.readDatabaseUrl), delayMs: config.delayMs });
  },
  checks: { postgres: () => pool.query('SELECT 1') },
  shutdown: async () => {
    if (readPool !== pool) await readPool.end();
    await pool.end();
  },
});

const vagaJson = (v) => ({
  id: v.id, especialidade: v.especialidade, dia: v.dia, hora: v.hora.slice(0, 5), estado: v.estado,
  ...(v.marcacao_id ? { marcacaoId: v.marcacao_id } : {}),
});
const SELECT_VAGA = 'SELECT id, especialidade, to_char(dia, \'YYYY-MM-DD\') AS dia, hora::text, estado, utente, marcacao_id FROM vagas';

svc.start((app) => {
  app.use(express.json());

  app.get('/agenda', async (req, res) => {
    const { especialidade, dia } = req.query;
    if (!ESPECIALIDADES[especialidade] || !/^\d{4}-\d{2}-\d{2}$/.test(dia || '')) {
      return reply(res, 400, { error: `esperado ?especialidade=${Object.keys(ESPECIALIDADES).join('|')}&dia=AAAA-MM-DD` });
    }
    const r = await readPool.query(`${SELECT_VAGA} WHERE especialidade = $1 AND dia = $2 AND estado = 'livre' ORDER BY hora`, [especialidade, dia]);
    reply(res, 200, { especialidade, nome: ESPECIALIDADES[especialidade], dia, livres: r.rowCount, vagas: r.rows.map(vagaJson) });
  });

  app.get('/vagas/:id', async (req, res) => {
    const r = await pool.query(`${SELECT_VAGA} WHERE id = $1`, [req.params.id]);
    if (!r.rowCount) return reply(res, 404, { error: 'vaga_inexistente' });
    reply(res, 200, vagaJson(r.rows[0]));
  });

  app.post('/vagas/:id/reservar', async (req, res) => {
    const { utente, marcacaoId } = req.body || {};
    if (typeof utente !== 'string' || typeof marcacaoId !== 'string' || !utente || !marcacaoId) {
      return reply(res, 400, { error: 'esperado {utente: string, marcacaoId: string}' });
    }
    await sleep(config.delayMs);
    // O coração do RNF1: só um pedido consegue mudar a vaga de livre para marcada.
    const r = await pool.query(
      `UPDATE vagas SET estado = 'marcada', utente = $2, marcacao_id = $3, atualizada_em = now()
        WHERE id = $1 AND estado = 'livre'`, [req.params.id, utente, marcacaoId]);
    if (r.rowCount === 1) {
      reservas.inc({ resultado: 'marcada' });
      return reply(res, 201, { status: 'marcada', vagaId: req.params.id, marcacaoId });
    }
    // 0 linhas: a vaga não existe, já é desta marcação (retry) ou é de outra pessoa.
    const atual = await pool.query('SELECT marcacao_id FROM vagas WHERE id = $1', [req.params.id]);
    if (!atual.rowCount) {
      reservas.inc({ resultado: 'inexistente' });
      return reply(res, 404, { error: 'vaga_inexistente' });
    }
    if (atual.rows[0].marcacao_id === marcacaoId) {
      reservas.inc({ resultado: 'repetida' });
      return reply(res, 200, { status: 'marcada', vagaId: req.params.id, marcacaoId, repetido: true });
    }
    reservas.inc({ resultado: 'ocupada' });
    reply(res, 409, { error: 'vaga_ocupada', vagaId: req.params.id });
  });

  app.post('/vagas/:id/libertar', async (req, res) => {
    const { marcacaoId } = req.body || {};
    if (typeof marcacaoId !== 'string' || !marcacaoId) return reply(res, 400, { error: 'esperado {marcacaoId: string}' });
    await pool.query(
      `UPDATE vagas SET estado = 'livre', utente = NULL, marcacao_id = NULL, atualizada_em = now()
        WHERE id = $1 AND marcacao_id = $2`, [req.params.id, marcacaoId]);
    // Libertar uma vaga que esta marcação já não tem é um retry: responde igual.
    reply(res, 200, { status: 'livre', vagaId: req.params.id });
  });

  app.get('/audit', async (req, res) => {
    const r = await pool.query(
      `SELECT count(*)::int AS vagas,
              count(*) FILTER (WHERE estado = 'livre')::int AS livres,
              count(*) FILTER (WHERE estado = 'marcada')::int AS marcadas
         FROM vagas WHERE ($1::date IS NULL OR dia = $1)`, [req.query.dia || null]);
    reply(res, 200, r.rows[0]);
  });
});
