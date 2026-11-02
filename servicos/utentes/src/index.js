'use strict';
// utentes: os dados pessoais, guardados no SQL Server. É o único serviço com credenciais para essa base de dados (RNF5).
//   GET /utentes/:numero   número de utente (9 dígitos) -> nome e telemóvel mascarado
// No arranque cria a base de dados, a tabela e UTENTES_SEED utentes fictícios (100000001, 100000002, ...).
const sql = require('mssql');
const { str, num, makeLogger, reply, retry, createService } = require('./lib');

const log = makeLogger('utentes');
const config = {
  port: num('PORT', 3003),
  host: str('MSSQL_HOST', 'localhost'),
  user: str('MSSQL_USER', 'sa'),
  password: str('MSSQL_PASSWORD', ''),
  database: str('MSSQL_DATABASE', 'utentes'),
  seed: num('UTENTES_SEED', 1000),
};

const connConfig = (database) => ({
  server: config.host, user: config.user, password: config.password, database,
  options: { encrypt: false, trustServerCertificate: true },
  pool: { max: 10 },
  connectionTimeout: 5000,
  requestTimeout: 5000,
});

let pool = null;

const mascarar = (telemovel) => `${telemovel.slice(0, 3)}***${telemovel.slice(-3)}`;

async function prepararBase() {
  // A base de dados tem de existir antes de abrirmos o pool nela: primeiro ligamos ao master.
  const master = await new sql.ConnectionPool(connConfig('master')).connect();
  try {
    await master.request().query(`IF DB_ID(N'${config.database}') IS NULL CREATE DATABASE [${config.database}]`);
  } finally {
    await master.close();
  }
  const p = await new sql.ConnectionPool(connConfig(config.database)).connect();
  try {
    await p.request().query(`
      IF OBJECT_ID(N'dbo.utentes') IS NULL
        CREATE TABLE dbo.utentes (
          numero     char(9)       NOT NULL PRIMARY KEY,
          nome       nvarchar(100) NOT NULL,
          telemovel  char(9)       NOT NULL,
          nascimento date          NOT NULL)`);
    // Utentes fictícios gerados no próprio servidor (sem dados reais).
    const r = await p.request().input('n', sql.Int, config.seed).query(`
      WITH nums AS (SELECT TOP (@n) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS i FROM sys.all_objects a CROSS JOIN sys.all_objects b)
      INSERT INTO dbo.utentes (numero, nome, telemovel, nascimento)
      SELECT CAST(100000000 + i AS char(9)), CONCAT(N'Utente ', i), CAST(910000000 + i AS char(9)), DATEADD(day, -(i * 37 % 30000), '2008-01-01')
        FROM nums WHERE NOT EXISTS (SELECT 1 FROM dbo.utentes u WHERE u.numero = CAST(100000000 + nums.i AS char(9)))`);
    if (r.rowsAffected[0]) log.info('utentes criados', { utentes: r.rowsAffected[0] });
  } catch (err) {
    await p.close();
    throw err;
  }
  return p;
}

const svc = createService({
  name: 'utentes',
  port: config.port,
  log,
  init: async () => {
    pool = await retry(prepararBase, { what: 'sqlserver', log, delayMs: 2000 }); // o SQL Server demora ~20 s a arrancar
    pool.on('error', (err) => log.warn('sqlserver pool error', { err: err.message }));
  },
  checks: { sqlserver: () => pool.request().query('SELECT 1') },
  shutdown: async () => { await pool?.close(); },
});

svc.start((app) => {
  app.get('/utentes/:numero', async (req, res) => {
    if (!/^\d{9}$/.test(req.params.numero)) return reply(res, 400, { error: 'numero de utente tem 9 digitos' });
    const r = await pool.request().input('numero', sql.Char(9), req.params.numero)
      .query('SELECT numero, nome, telemovel FROM dbo.utentes WHERE numero = @numero');
    if (!r.recordset.length) return reply(res, 404, { error: 'utente_inexistente' });
    const u = r.recordset[0];
    reply(res, 200, { numero: u.numero, nome: u.nome, telemovel: mascarar(u.telemovel) });
  });
});
