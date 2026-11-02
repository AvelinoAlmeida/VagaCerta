// Teste de ponta a ponta da VagaCerta. Corre dentro da rede do Compose (ver testes/e2e.sh) e entra pela gateway,
// como um utente; os serviços internos só são chamados diretamente para auditar o resultado.
//
//   node e2e.mjs funcional            fluxo completo, idempotência, concorrência, desmarcação
//   node e2e.mjs sem-rabbit           marca com o RabbitMQ parado; escreve o marcacaoId em /resultados/.sem-rabbit
//   node e2e.mjs sem-rabbit-verifica  depois de o RabbitMQ voltar: o SMS dessa marcação tem de chegar
//
// Sem dependências: só fetch e node:assert. Sai com código 1 se algum passo falhar.
import { randomUUID } from 'node:crypto';
import { writeFileSync, readFileSync } from 'node:fs';

const GATEWAY = process.env.GATEWAY_URL || 'http://gateway:8080';
const AGENDA = process.env.AGENDA_URL || 'http://agenda:3001';
const MARCACOES = process.env.MARCACOES_URL || 'http://marcacoes:3002';
const UTENTES = process.env.UTENTES_URL || 'http://utentes:3003';
const NOTIFICACOES = process.env.NOTIFICACOES_URL || 'http://notificacoes:3004';
const DIA = process.env.DIA || '2026-12-02';
const CONCORRENTES = Number(process.env.CONCORRENTES || 100);

const resultados = [];
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function http(method, url, { body, headers = {} } = {}) {
  const t0 = performance.now();
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: AbortSignal.timeout(15000),
  });
  const json = await res.json().catch(() => ({}));
  return { status: res.status, body: json, headers: res.headers, ms: performance.now() - t0 };
}

const marcar = (utente, vagaId, chave = randomUUID()) =>
  http('POST', `${GATEWAY}/marcacoes`, { body: { utente, vagaId }, headers: { 'idempotency-key': chave } });

async function passo(nome, fn) {
  const t0 = performance.now();
  try {
    const detalhe = await fn();
    resultados.push({ nome, ok: true, ms: performance.now() - t0, detalhe });
    console.log(`  ✓ ${nome}${detalhe ? `  — ${detalhe}` : ''}`);
  } catch (err) {
    resultados.push({ nome, ok: false, ms: performance.now() - t0, detalhe: err.message });
    console.log(`  ✗ ${nome}  — ${err.message}`);
  }
}

function espera(cond, msg) {
  if (!cond) throw new Error(msg);
}
const igual = (atual, esperado, o) => espera(atual === esperado, `${o}: esperado ${JSON.stringify(esperado)}, obtido ${JSON.stringify(atual)}`);

async function aguardar(fn, { ms = 20000, passo: p = 250 } = {}) {
  const fim = Date.now() + ms;
  for (;;) {
    const v = await fn();
    if (v) return v;
    if (Date.now() > fim) return null;
    await sleep(p);
  }
}

// Escolhe vagas livres (de qualquer especialidade) para o teste não depender de ser a primeira execução.
async function vagasLivres(n) {
  const livres = [];
  for (const esp of ['CARD', 'MGF', 'ORTO', 'OFT']) {
    const r = await http('GET', `${GATEWAY}/agenda?especialidade=${esp}&dia=${DIA}`);
    livres.push(...(r.body.vagas || []).map((v) => v.id));
  }
  espera(livres.length >= n, `só há ${livres.length} vagas livres em ${DIA}; recrie os volumes (docker compose down -v)`);
  return livres.slice(0, n);
}
const espDe = (vagaId) => vagaId.split('-')[4];
const livresEm = async (esp) => (await http('GET', `${GATEWAY}/agenda?especialidade=${esp}&dia=${DIA}`)).body;

async function funcional() {
  let vaga, vagaDisputada, livresAntes, marcacao, chave, smsAntes;

  console.log('\n1. Arranque');
  await passo('gateway e os 4 serviços respondem /ready', async () => {
    igual((await http('GET', `${GATEWAY}/health`)).status, 200, 'gateway');
    for (const [nome, url] of Object.entries({ agenda: AGENDA, marcacoes: MARCACOES, utentes: UTENTES, notificacoes: NOTIFICACOES })) {
      const r = await http('GET', `${url}/ready`);
      igual(r.status, 200, `${nome} /ready (${JSON.stringify(r.body.checks)})`);
    }
    return 'gateway, agenda, marcacoes, utentes, notificacoes';
  });

  console.log('\n2. Consultar vagas (RF1)');
  await passo('GET /agenda devolve as vagas livres do dia', async () => {
    [vaga, vagaDisputada] = await vagasLivres(2);
    const a = await livresEm(espDe(vaga));
    livresAntes = a.livres;
    espera(a.vagas.every((v) => v.estado === 'livre' && v.dia === DIA), 'só deviam vir vagas livres do dia pedido');
    return `${a.nome}: ${a.livres} livres; vaga escolhida ${vaga}`;
  });
  await passo('pedido inválido em /agenda dá 400', async () => {
    igual((await http('GET', `${GATEWAY}/agenda?especialidade=XPTO&dia=${DIA}`)).status, 400, 'status');
  });

  console.log('\n3. Validações da marcação');
  await passo('sem Idempotency-Key dá 400', async () => {
    igual((await http('POST', `${GATEWAY}/marcacoes`, { body: { utente: '100000001', vagaId: vaga } })).status, 400, 'status');
  });
  await passo('utente inexistente dá 422', async () => {
    const r = await marcar('999999999', vaga);
    igual(r.status, 422, 'status');
    igual(r.body.motivo, 'utente_inexistente', 'motivo');
  });
  await passo('vaga inexistente dá 404', async () => {
    igual((await marcar('100000001', `V-${DIA}-CARD-2359`)).status, 404, 'status');
  });

  console.log('\n4. Marcar (RF2)');
  await passo('POST /marcacoes dá 201 e a marcação fica confirmada', async () => {
    chave = randomUUID();
    smsAntes = (await http('GET', `${NOTIFICACOES}/stats`)).body.sms_enviados;
    const r = await marcar('100000001', vaga, chave);
    igual(r.status, 201, `status (${JSON.stringify(r.body)})`);
    igual(r.body.estado, 'confirmada', 'estado');
    marcacao = r.body;
    return `marcação ${marcacao.id.slice(0, 8)} em ${Math.round(r.ms)} ms`;
  });
  await passo('a agenda tem a vaga marcada por esta marcação', async () => {
    const v = await http('GET', `${AGENDA}/vagas/${vaga}`);
    igual(v.body.estado, 'marcada', 'estado da vaga');
    igual(v.body.marcacaoId, marcacao.id, 'marcacaoId na agenda');
    const a = await livresEm(espDe(vaga));
    igual(a.livres, livresAntes - 1, 'vagas livres');
    espera(!a.vagas.some((v2) => v2.id === vaga), 'a vaga ainda aparece como livre');
    return `livres ${livresAntes} → ${a.livres}`;
  });

  console.log('\n5. Idempotência');
  await passo('repetir o pedido com a mesma chave devolve a mesma marcação', async () => {
    const r = await marcar('100000001', vaga, chave);
    igual(r.status, 201, 'status');
    igual(r.body.id, marcacao.id, 'id');
    igual(r.headers.get('idempotent-replayed'), 'true', 'cabeçalho idempotent-replayed');
  });
  await passo('reutilizar a chave noutro pedido dá 422', async () => {
    const r = await marcar('100000001', vagaDisputada, chave);
    igual(r.status, 422, 'status');
    igual(r.body.error, 'chave_reutilizada', 'erro');
  });
  await passo(`20 pedidos simultâneos com a mesma chave criam uma só marcação`, async () => {
    const [v] = await vagasLivres(3).then((l) => l.slice(2));
    const k = randomUUID();
    const rs = await Promise.all(Array.from({ length: 20 }, () => marcar('100000050', v, k)));
    const ids = new Set(rs.filter((r) => r.status === 201).map((r) => r.body.id));
    const estados = rs.map((r) => r.status);
    espera(ids.size === 1, `ids distintos: ${ids.size} (estados ${[...new Set(estados)]})`);
    espera(estados.every((s) => s === 201 || s === 503), `estados inesperados: ${[...new Set(estados)]}`);
    const vg = await http('GET', `${AGENDA}/vagas/${v}`);
    igual(vg.body.marcacaoId, [...ids][0], 'marcacaoId na agenda');
    return `${estados.filter((s) => s === 201).length}×201, 1 marcação (${v})`;
  });

  console.log('\n6. Confirmação por SMS (RF4, RNF4)');
  await passo('o SMS chega uma vez através do RabbitMQ', async () => {
    const t0 = Date.now();
    const sms = await aguardar(async () => {
      const r = await http('GET', `${NOTIFICACOES}/sms/${marcacao.id}`);
      return r.status === 200 && r.body;
    });
    espera(sms, 'o SMS não chegou em 20 s');
    const m = await http('GET', `${GATEWAY}/marcacoes/${marcacao.id}`);
    igual(m.body.notificada, true, 'marcação notificada');
    return `"${sms.texto}" (${Date.now() - t0} ms)`;
  });

  console.log(`\n7. Concorrência (RNF1): ${CONCORRENTES} utentes na mesma vaga ao mesmo tempo`);
  await passo('exatamente um 201, os restantes 409, nenhum 5xx', async () => {
    const utentes = Array.from({ length: CONCORRENTES }, (_, i) => String(100000101 + i));
    const t0 = performance.now();
    const rs = await Promise.all(utentes.map((u) => marcar(u, vagaDisputada)));
    const ms = performance.now() - t0;
    const conta = (s) => rs.filter((r) => r.status === s).length;
    const outros = rs.filter((r) => r.status !== 201 && r.status !== 409).map((r) => r.status);
    const lat = rs.map((r) => r.ms).sort((a, b) => a - b);
    const p95 = lat[Math.floor(lat.length * 0.95) - 1];
    igual(conta(201), 1, 'respostas 201');
    igual(conta(409), CONCORRENTES - 1, 'respostas 409');
    igual(outros.length, 0, `outras respostas (${outros})`);
    return `1×201, ${conta(409)}×409 em ${Math.round(ms)} ms (p95 ${Math.round(p95)} ms)`;
  });
  await passo('auditoria: nenhuma vaga com duas marcações', async () => {
    const a = await http('GET', `${MARCACOES}/audit`);
    igual(a.body.vagas_duplicadas, 0, `vagas duplicadas (${JSON.stringify(a.body.duplicados)})`);
    const v = await http('GET', `${AGENDA}/vagas/${vagaDisputada}`);
    igual(v.body.estado, 'marcada', 'vaga disputada');
    return `${a.body.confirmadas} confirmadas, ${a.body.rejeitadas} rejeitadas, 0 duplicadas`;
  });

  console.log('\n8. Desmarcar (RF3)');
  await passo('DELETE /marcacoes/:id cancela e devolve a vaga a livre', async () => {
    const antes = (await livresEm(espDe(vaga))).livres; // os passos 5 e 7 também marcaram vagas desta especialidade
    const r = await http('DELETE', `${GATEWAY}/marcacoes/${marcacao.id}`);
    igual(r.status, 200, 'status');
    igual(r.body.estado, 'cancelada', 'estado');
    const v = await http('GET', `${AGENDA}/vagas/${vaga}`);
    igual(v.body.estado, 'livre', 'estado da vaga');
    const depois = (await livresEm(espDe(vaga))).livres;
    igual(depois, antes + 1, 'vagas livres');
    return `livres ${antes} → ${depois}`;
  });
  await passo('desmarcar outra vez é inofensivo (200)', async () => {
    const r = await http('DELETE', `${GATEWAY}/marcacoes/${marcacao.id}`);
    igual(r.status, 200, 'status');
    igual(r.body.estado, 'cancelada', 'estado');
  });
  await passo('a vaga libertada pode ser marcada por outro utente', async () => {
    const r = await marcar('100000002', vaga);
    igual(r.status, 201, 'status');
    await http('DELETE', `${GATEWAY}/marcacoes/${r.body.id}`); // deixa a vaga livre para a próxima execução
  });

  console.log('\n9. Fecho');
  await passo('SMS sem duplicados: um por marcação confirmada', async () => {
    await sleep(1500);
    const depois = (await http('GET', `${NOTIFICACOES}/stats`)).body.sms_enviados;
    const a = (await http('GET', `${MARCACOES}/audit`)).body;
    igual(a.por_notificar, 0, 'marcações por notificar');
    // Neste teste confirmaram-se 4 marcações: a principal, a da chave repetida, a disputada e a da vaga libertada.
    igual(depois - smsAntes, 4, 'SMS novos');
    return `${depois - smsAntes} SMS novos para 4 marcações`;
  });
}

async function semRabbit() {
  console.log('\nE5 (versão curta): marcar com o RabbitMQ parado');
  await passo('a marcação responde 201 sem RabbitMQ', async () => {
    const [vaga] = await vagasLivres(1);
    const r = await marcar('100000003', vaga);
    igual(r.status, 201, `status (${JSON.stringify(r.body)})`);
    igual(r.body.notificada, false, 'notificada');
    writeFileSync('/resultados/.sem-rabbit', r.body.id);
    return `marcação ${r.body.id.slice(0, 8)} em ${Math.round(r.ms)} ms, fica no outbox`;
  });
  await passo('a marcação fica por notificar no outbox', async () => {
    const a = (await http('GET', `${MARCACOES}/audit`)).body;
    espera(a.por_notificar >= 1, `por_notificar = ${a.por_notificar}`);
    return `por_notificar = ${a.por_notificar}`;
  });
}

async function semRabbitVerifica() {
  console.log('\nE5 (versão curta): depois de o RabbitMQ voltar');
  const id = readFileSync('/resultados/.sem-rabbit', 'utf8').trim();
  await passo('o outbox republica e o SMS chega', async () => {
    const t0 = Date.now();
    const sms = await aguardar(async () => (await http('GET', `${NOTIFICACOES}/sms/${id}`)).status === 200, { ms: 60000, passo: 500 });
    espera(sms, 'o SMS não chegou em 60 s');
    const m = await http('GET', `${GATEWAY}/marcacoes/${id}`);
    igual(m.body.notificada, true, 'notificada');
    return `SMS entregue ${((Date.now() - t0) / 1000).toFixed(1)} s depois de o RabbitMQ voltar`;
  });
  await passo('outbox vazio', async () => {
    const a = (await aguardar(async () => {
      const r = (await http('GET', `${MARCACOES}/audit`)).body;
      return r.por_notificar === 0 && r;
    }, { ms: 10000 }));
    espera(a, 'ainda há marcações por notificar');
  });
}

const fase = process.argv[2] || 'funcional';
await { funcional, 'sem-rabbit': semRabbit, 'sem-rabbit-verifica': semRabbitVerifica }[fase]();

const falhados = resultados.filter((r) => !r.ok).length;
writeFileSync(`/resultados/.e2e-${fase}.json`, JSON.stringify(resultados, null, 2));
console.log(`\n${resultados.length - falhados}/${resultados.length} passos OK${falhados ? `, ${falhados} falharam` : ''}`);
process.exit(falhados ? 1 : 0);
