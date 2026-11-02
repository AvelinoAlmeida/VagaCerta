'use strict';
// Clientes HTTP de utentes e agenda: timeout por tentativa + retries com backoff exponencial e jitter.
// Cada tentativa de reserva leva o MESMO marcacaoId, por isso a agenda reconhece o retry e não marca duas vezes.
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Faz o pedido com retries. Devolve { status, body, tentativas } para qualquer resposta abaixo de 500
 * (409 vaga ocupada ou 404 não valem a pena repetir). Repete em erros de rede, timeouts e 5xx;
 * esgotadas as tentativas lança o erro (com err.tentativas e err.timeout).
 */
async function pedido({ url, method = 'GET', body, timeoutMs, retries, backoffMs, requestId, fetchFn = fetch, onRetry = () => {} }) {
  for (let tentativa = 1; ; tentativa++) {
    let falha;
    try {
      const res = await fetchFn(url, {
        method,
        headers: { 'content-type': 'application/json', 'x-request-id': requestId },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      });
      const json = await res.json().catch(() => ({}));
      if (res.status < 500) return { status: res.status, body: json, tentativas: tentativa };
      falha = Object.assign(new Error(`${url} respondeu ${res.status}`), { timeout: false });
    } catch (err) {
      falha = Object.assign(err, { timeout: err.name === 'TimeoutError' || err.name === 'AbortError' });
    }
    if (tentativa > retries) throw Object.assign(falha, { tentativas: tentativa });
    onRetry(tentativa, falha);
    // Full jitter: espera aleatória entre 0 e base * 2^(n-1). Evita que os retries de 300 utentes cheguem todos juntos.
    await sleep(Math.random() * backoffMs * 2 ** (tentativa - 1));
  }
}

function clientes({ utentesUrl, agendaUrl, timeoutMs, retries, backoffMs, onRetry }) {
  const base = { timeoutMs, retries, backoffMs };
  return {
    utente: (numero, requestId) => pedido({ ...base, url: `${utentesUrl}/utentes/${encodeURIComponent(numero)}`, requestId, onRetry: onRetry('utentes') }),
    reservar: (vagaId, utente, marcacaoId, requestId) => pedido({
      ...base, url: `${agendaUrl}/vagas/${encodeURIComponent(vagaId)}/reservar`, method: 'POST', body: { utente, marcacaoId }, requestId, onRetry: onRetry('agenda'),
    }),
    libertar: (vagaId, marcacaoId, requestId) => pedido({
      ...base, url: `${agendaUrl}/vagas/${encodeURIComponent(vagaId)}/libertar`, method: 'POST', body: { marcacaoId }, requestId, onRetry: onRetry('agenda'),
    }),
  };
}

module.exports = { pedido, clientes };
