# VagaCerta

**Marcação de consultas distribuída, preparada para o pico de procura.**

Numa unidade de saúde, as vagas de consulta do dia abrem às 08:00 e, nesse minuto, milhares de utentes tentam marcar ao mesmo tempo. A VagaCerta é uma plataforma de marcação (fictícia) desenhada para esse momento:

- **aguenta o pico** — serviços sem estado, que escalam horizontalmente;
- **nunca dá a mesma vaga a duas pessoas** — a reserva é decidida por um único `UPDATE` condicional;
- **não perde confirmações** — cada marcação gera um SMS, entregue pelo menos uma vez e sem duplicados;
- **protege os dados pessoais** — só um serviço tem acesso à base de dados dos utentes.

A lógica de negócio é propositadamente mínima: o foco está na infraestrutura — como se monta, configura, opera e mede.

## Arquitetura

```
                    ┌──────────────┐
  Utente ── HTTP ──►│   gateway    │  GET /agenda · POST /marcacoes
                    └──┬────────┬──┘
                       │        │
              ┌────────▼──┐  ┌──▼──────────┐  GET /utentes/:n  ┌───────────┐     ┌────────────┐
              │  agenda   │◄─┤  marcacoes  ├──────────────────►│  utentes  ├────►│ SQL Server │
              └─────┬─────┘  └──┬───────┬──┘                   └───────────┘     └────────────┘
                    │           │       │ marcacao.criada
                    │           │       ▼
              ┌─────▼───────────▼┐  ┌──────────┐   ┌──────────────┐
              │     Postgres     │  │ RabbitMQ ├──►│ notificacoes │──► SMS (simulado)
              └──────────────────┘  └──────────┘   └──────────────┘
```

| Componente | Papel |
|---|---|
| `gateway` | Ponto de entrada. No Compose é um nginx; no Kubernetes, o Ingress (Traefik). |
| `agenda` | Vagas de consulta: a única fonte de verdade sobre quem tem cada vaga. |
| `marcacoes` | Orquestra a marcação: valida o utente, reserva a vaga, publica a confirmação. |
| `utentes` | Dados pessoais dos utentes, no SQL Server. |
| `notificacoes` | Consome a fila `marcacao.criada` e envia o SMS de confirmação. |
| Postgres | Uma base de dados por serviço (`agenda`, `marcacoes`, `notificacoes`). |
| SQL Server 2022 | Base de dados `utentes`, acessível apenas ao serviço `utentes`. |
| RabbitMQ | Fila durável `marcacao.criada`, com dead-letter queue. |

Cada vaga tem um identificador legível — `V-<dia>-<especialidade>-<hora>`, por exemplo `V-2026-12-02-CARD-0815` — e um estado: `livre`, `marcada` ou `cancelada`. Especialidades: `CARD` Cardiologia, `MGF` Medicina Geral e Familiar, `ORTO` Ortopedia, `OFT` Oftalmologia.

## Como funciona uma marcação

```
POST /marcacoes  (Idempotency-Key: <uuid>)  {"utente": "100000001", "vagaId": "V-2026-12-02-CARD-0815"}

marcacoes ─1─► grava a chave com um marcacaoId novo (estado pendente)
          ─2─► utentes   GET /utentes/100000001                              404 → 422
          ─3─► agenda    POST /vagas/{id}/reservar  timeout 300 ms, 2 retries com jitter
                           UPDATE vagas SET estado='marcada' WHERE id=$1 AND estado='livre'
                           0 linhas → 409 vaga ocupada
          ─4─► confirma a marcação e publica marcacao.criada ─► RabbitMQ ─► notificacoes
          ◄─── 201 Created
```

As garantias vêm de três mecanismos simples:

| Garantia | Mecanismo |
|---|---|
| Uma vaga, uma pessoa | O `UPDATE ... WHERE estado='livre'` é atómico: com 100 pedidos simultâneos, só um muda a linha. |
| Retries sem duplicar | A `Idempotency-Key` fica gravada antes de se chamar a agenda; o mesmo `marcacaoId` segue em todas as tentativas, e a agenda reconhece-o. |
| Confirmação garantida | Outbox: a marcação fica `notificada=false` até o RabbitMQ confirmar a mensagem. Se o RabbitMQ estiver em baixo, a marcação responde 201 na mesma e um ciclo de fundo republica-a quando ele voltar. |
| SMS sem duplicados | O consumidor regista cada `marcacaoId` com `ON CONFLICT DO NOTHING`; mensagens repetidas são ignoradas. |

## Começar

Requisitos: Docker com Compose v2. Não é preciso Node.js instalado.

```bash
docker compose up -d --build --wait      # ~20 s depois do primeiro build
```

| O quê | Onde |
|---|---|
| API | http://localhost:8081 |
| Consola do RabbitMQ | http://localhost:15672 (`app` / `app`) |

No arranque, a agenda cria as vagas de 02/12/2026 e 03/12/2026 (4 especialidades × 16 horários, das 08:00 às 11:45) e o serviço `utentes` cria 1000 utentes fictícios, de `100000001` a `100001000`.

```bash
# vagas livres
curl 'http://localhost:8081/agenda?especialidade=CARD&dia=2026-12-02'

# marcar
curl -X POST http://localhost:8081/marcacoes -H 'content-type: application/json' \
     -H "Idempotency-Key: $(uuidgen)" -d '{"utente":"100000001","vagaId":"V-2026-12-02-CARD-0815"}'

# desmarcar
curl -X DELETE http://localhost:8081/marcacoes/<id>

docker compose down -v                   # para tudo e apaga os dados
```

### Contentores

Os nomes seguem o padrão `vagacerta-<camada>-<nome>`, para se perceber logo o papel de cada um em `docker ps`:

| Camada | Contentor | Imagem | Porta interna |
|---|---|---|---|
| entrada | `vagacerta-entrada-gateway` | `nginx:alpine` | 8080 (exposta em 8081) |
| serviço | `vagacerta-servico-agenda` | `vagacerta/agenda:0.1` | 3001 |
| serviço | `vagacerta-servico-marcacoes` | `vagacerta/marcacoes:0.1` | 3002 |
| serviço | `vagacerta-servico-utentes` | `vagacerta/utentes:0.1` | 3003 |
| serviço | `vagacerta-servico-notificacoes` | `vagacerta/notificacoes:0.1` | 3004 |
| dados | `vagacerta-dados-postgres` | `postgres:17-alpine` | 5432 |
| dados | `vagacerta-dados-sqlserver` | `mssql/server:2022-latest` | 1433 |
| fila | `vagacerta-fila-rabbitmq` | `rabbitmq:4-management-alpine` | 5672 (consola em 15672) |

Volumes `vagacerta-volume-postgres`, `vagacerta-volume-sqlserver`, `vagacerta-volume-rabbitmq`; rede `vagacerta-rede`. Dentro da rede, os serviços chamam-se pelo nome curto (`agenda`, `postgres`, …), tal como no Kubernetes.

## API

Pela gateway:

| Método | Caminho | Descrição | Respostas |
|---|---|---|---|
| `GET` | `/agenda?especialidade=CARD&dia=2026-12-02` | Vagas livres de uma especialidade num dia | 200, 400 |
| `POST` | `/marcacoes` | Marca uma vaga. Corpo `{utente, vagaId}`, cabeçalho `Idempotency-Key` obrigatório | 201, 400, 404 vaga, 409 ocupada, 422 utente/chave, 503 |
| `GET` | `/marcacoes/:id` | Estado de uma marcação | 200, 404 |
| `DELETE` | `/marcacoes/:id` | Desmarca e devolve a vaga a livre (idempotente) | 200, 404, 409 |

Repetir um `POST` com a mesma chave devolve a resposta original, com o cabeçalho `idempotent-replayed: true`.

Internas (só dentro da rede):

| Serviço | Rotas |
|---|---|
| `agenda` | `GET /vagas/:id`, `POST /vagas/:id/reservar`, `POST /vagas/:id/libertar`, `GET /audit` |
| `marcacoes` | `GET /audit` — contagens e vagas com mais de uma marcação (deve ser sempre 0) |
| `utentes` | `GET /utentes/:numero` — nome e telemóvel mascarado |
| `notificacoes` | `GET /sms/:marcacaoId`, `GET /stats` |

Todos os serviços têm `GET /health` (vivo), `GET /ready` (dependências OK) e `GET /metrics` (Prometheus). Os registos são JSON numa linha, com o `x-request-id` propagado entre serviços.

## Configuração

Variáveis principais (valores por omissão do `compose.yaml`):

| Variável | Serviço | Omissão | Efeito |
|---|---|---|---|
| `GATEWAY_PORT` | gateway | `8081` | Porta exposta no anfitrião |
| `TIMEOUT_MS` / `RETRIES` | marcacoes | `300` / `2` | Timeout por tentativa e retries para utentes e agenda |
| `OUTBOX_INTERVAL_MS` | marcacoes | `2000` | Intervalo do ciclo que republica confirmações pendentes |
| `SEED_DIAS` | agenda | `2026-12-02,2026-12-03` | Dias com vagas criadas no arranque |
| `RESERVA_DELAY_MS` | agenda | `0` | Atraso artificial na reserva (testes de timeouts) |
| `UTENTES_SEED` | utentes | `1000` | Utentes fictícios criados no arranque |
| `SMS_DELAY_MS` / `SMS_FAIL_RATE` | notificacoes | `0` / `0` | Operadora de SMS lenta ou com falhas |
| `POSTGRES_PASSWORD`, `MSSQL_SA_PASSWORD`, `RABBITMQ_PASSWORD` | — | valores de desenvolvimento | Credenciais (Secrets no Kubernetes) |

## Testes

```bash
bash testes/e2e.sh              # sobe o Compose, corre o teste e grava resultados/e2e-<data>.txt
LIMPAR=1 bash testes/e2e.sh     # começa de volumes vazios
```

O teste de ponta a ponta corre num contentor `node:22-alpine` ligado à rede e entra pela gateway, como um utente. Cobre:

- consulta de vagas, marcação, desmarcação e validações;
- idempotência (pedido repetido, chave reutilizada, 20 pedidos simultâneos com a mesma chave);
- **100 utentes em simultâneo na mesma vaga**: exatamente um 201, 99 × 409, nenhuma vaga duplicada;
- confirmação por SMS, uma por marcação;
- marcar com o RabbitMQ parado e confirmar que o SMS chega quando ele volta.

## Requisitos

| ID | Requisito | Alvo |
|---|---|---|
| RF1 | Consultar vagas livres por especialidade e dia | — |
| RF2 | Marcar uma vaga para um utente (número de utente) | — |
| RF3 | Desmarcar uma consulta e devolver a vaga ao estado livre | — |
| RF4 | Enviar uma confirmação (SMS simulado) após cada marcação | — |
| RF5 | Painel de saúde do sistema e do SQL Server | — |
| RNF1 | Correção sob carga: nenhuma vaga marcada duas vezes | 0 duplicados com 300 VU |
| RNF2 | Disponibilidade das marcações (SLO) | ≥ 99,5 % de respostas não 5xx |
| RNF3 | Latência das marcações no pico (SLO) | p95 < 500 ms |
| RNF4 | Confirmação entregue pelo menos uma vez, sem duplicar | consumidor idempotente |
| RNF5 | Dados de utentes só acessíveis ao serviço `utentes` | NetworkPolicy e Secret |

## Estrutura

```
.
├── compose.yaml         a aplicação completa em Docker Compose
├── gateway/             nginx com as mesmas rotas do Ingress
├── servicos/            agenda, marcacoes, utentes, notificacoes (Node.js 22 + Express)
├── sql/                 esquemas e consultas
├── testes/              teste de ponta a ponta
├── carga/               cenários de carga k6
├── caos/                injeção de falhas
├── k8s/
│   ├── base/            deployments, services, ingress
│   ├── dados/           postgres, sqlserver, rabbitmq
│   ├── observabilidade/ prometheus, grafana, exporter
│   └── seguranca/       networkpolicy, rbac
└── resultados/          relatórios dos testes
```

## Roteiro

- [x] Quatro serviços, Docker Compose e teste de ponta a ponta
- [ ] Kubernetes (k3d): Deployments, Ingress, Postgres, SQL Server
- [ ] Réplica de leitura do Postgres para a consulta de disponibilidade
- [ ] Cenário de carga do pico das 08:00 (k6, 300 VU)
- [ ] Observabilidade: Prometheus, Grafana, alertas de SLO, exporter do SQL Server
- [ ] Escalamento automático (HPA) e sondas
- [ ] Segurança: NetworkPolicy, Secrets, RBAC
- [ ] Testes de resiliência: perda de Pods, broker em baixo, base de dados em baixo

## Licença

[MIT](LICENSE) · Avelino Almeida, 2026
