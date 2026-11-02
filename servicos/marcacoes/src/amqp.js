'use strict';
// RabbitMQ connection with automatic reconnection, and the queue topology shared by producer and consumer.
// Identical copy in marcacoes and notificacoes (both must declare the queues with the same arguments).
const amqplib = require('amqplib');

const QUEUE = 'marcacao.criada';
const DLQ = 'marcacao.criada.dlq'; // messages rejected with requeue=false end up here (dead-letter exchange = default exchange)

async function assertTopology(ch) {
  await ch.assertQueue(DLQ, { durable: true });
  await ch.assertQueue(QUEUE, { durable: true, arguments: { 'x-dead-letter-exchange': '', 'x-dead-letter-routing-key': DLQ } });
}

/**
 * connect(): resolves when the first channel is open (retrying until the broker is up); afterwards it reconnects
 * by itself if the connection drops. onChannel(ch) runs on every (re)connection (e.g. to start consuming).
 */
function amqpConnection({ url, log, confirm = false, onChannel = async () => {} }) {
  let conn = null;
  let channel = null;
  let closing = false;

  async function open() {
    for (;;) {
      if (closing) return;
      try {
        conn = await amqplib.connect(url);
        conn.on('error', (err) => log.warn('amqp connection error', { err: err.message }));
        conn.on('close', () => {
          channel = null;
          if (closing) return;
          log.warn('amqp connection closed, reconnecting');
          setTimeout(open, 1000);
        });
        const ch = confirm ? await conn.createConfirmChannel() : await conn.createChannel();
        ch.on('error', (err) => log.warn('amqp channel error', { err: err.message }));
        ch.on('close', () => { // a dead channel on a live connection: drop the connection so the reconnect logic starts over
          if (channel === ch) { channel = null; if (!closing) conn.close().catch(() => {}); }
        });
        await assertTopology(ch);
        await onChannel(ch);
        channel = ch;
        log.info('amqp connected');
        return;
      } catch (err) {
        log.warn('amqp connect failed, retrying', { err: err.message });
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
  }

  return {
    connect: open,
    isUp: () => channel !== null,
    channel: () => channel,
    reset: () => { conn?.close().catch(() => {}); }, // drop the connection; the close handler reconnects and re-creates the channel
    async close() {
      closing = true;
      try { await channel?.close(); } catch { /* already closed */ }
      try { await conn?.close(); } catch { /* already closed */ }
    },
  };
}

module.exports = { QUEUE, DLQ, amqpConnection };
