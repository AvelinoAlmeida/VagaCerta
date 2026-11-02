# VagaCerta

**Marcação de consultas distribuída sob pico de procura**

Trabalho prático de Computação Distribuída e em Nuvem · MEI · ESTG P.PORTO · 2026/27
Avelino Almeida · Docente: Ricardo Costa · Defesa: 16/12/2026

## O problema

Numa unidade de saúde, as vagas de consulta do dia abrem às 08:00. Nesse minuto, milhares de utentes tentam marcar ao mesmo tempo. O sistema tem de:

- aguentar o pico;
- nunca dar a mesma vaga a duas pessoas (uma vaga marcada duas vezes é um utente mandado embora no balcão);
- confirmar cada marcação (uma confirmação perdida é uma consulta faltada);
- proteger os dados pessoais dos utentes.

A VagaCerta é uma plataforma fictícia que serve de caso de estudo para os temas T01 a T06 da unidade curricular. A lógica de negócio é mínima: o trabalho está em montar, configurar, operar e medir a infraestrutura.

Cada vaga tem um identificador (por exemplo `V-2026-12-02-CARD-0815`) e um estado: `livre`, `marcada` ou `cancelada`. Especialidades: Cardiologia, Medicina Geral e Familiar, Ortopedia e Oftalmologia.

## Arquitetura

Tudo corre num cluster k3d local (1 server + 2 agents), no namespace `vagacerta`.

| Componente | Papel | Réplicas |
|---|---|---|
| Ingress (Traefik) | Entrada: `POST /marcacoes`, `GET /agenda` | — |
| `marcacoes` | Orquestra a marcação: valida o utente, reserva na agenda, publica a confirmação | HPA 2–6 |
| `agenda` | Vagas: única fonte de verdade | HPA 2–6 |
| `utentes` | Dados pessoais | 2 |
| `notificacoes` | Consome a fila e envia o SMS (simulado) | 1–2 |
| Postgres primário + réplica | Escritas no primário; consulta de disponibilidade na réplica | 1 + 1 |
| SQL Server 2022 | Base de dados dos utentes, numa zona de rede isolada | 1 |
| RabbitMQ | Fila durável `marcacao.criada` | 1 |
| Prometheus, Grafana, Alertmanager, `sql_exporter` | Métricas, painéis, alertas do SLO e saúde do SQL Server | — |

Uma marcação, do clique ao SMS:

```
Utente ─POST /marcacoes (Idempotency-Key)─► marcacoes ─GET─► utentes ─► SQL Server
                                               │
                                               ├─POST /vagas/{id}/reservar─► agenda ─► Postgres primário
                                               │  timeout 300 ms · 2 retries      UPDATE vagas SET estado='marcada'
                                               │  com jitter                      WHERE id=$1 AND estado='livre'
                                               │                                  (0 linhas → 409 vaga ocupada)
                                               ├─publica marcacao.criada─► RabbitMQ ─► notificacoes (dedup por id)
                                               ▼
                                      201 Created ao utente
```

O `UPDATE` condicional garante que só um pedido muda a vaga de `livre` para `marcada`. A chave de idempotência garante que os retries não marcam duas vezes. A confirmação é assíncrona: se o RabbitMQ ou as notificações falharem, a marcação continua a funcionar.

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

## Experiências

Cada experiência segue o método da engenharia do caos (estado estacionário, hipótese, injeção, medição) e corre com o mesmo cenário k6 do pico das 08:00.

| ID | Hipótese | Injeção | Métrica |
|---|---|---|---|
| E1 | Nenhuma vaga é marcada duas vezes | 300 VU na mesma vaga | duplicados |
| E2 | Retries sem idempotência duplicam marcações | atraso de 400 ms na agenda | marcações por utente |
| E3 | O HPA escala de 2 para 4+ réplicas em menos de 2 min | rampa de 0 a 300 VU | réplicas, p95 |
| E4 | Perder Pods da agenda não viola o SLO | apagar Pods sob carga | % 5xx, orçamento de erro |
| E5 | Sem RabbitMQ as marcações continuam | parar o RabbitMQ 2 min | marcações, fila |
| E6 | A réplica mostra vagas já ocupadas por instantes | carga de escrita | atraso de replicação |
| E7 | Só o serviço `utentes` chega ao SQL Server | Pod de teste com `sqlcmd` | ligação recusada |
| E8 | O painel deteta o SQL Server em baixo em menos de 1 min | escalar o SQL Server a 0 | tempo até ao alerta |

## Estrutura

```
.
├── servicos/            agenda, marcacoes, utentes, notificacoes
├── k8s/
│   ├── base/            deployments, services, ingress
│   ├── dados/           postgres, sqlserver, rabbitmq
│   ├── observabilidade/ prometheus, grafana, exporter
│   └── seguranca/       networkpolicy, rbac
├── sql/                 esquemas e consultas às DMV
├── carga/               cenários k6
├── caos/                scripts das experiências E1 a E8
└── resultados/          csv e capturas
```

As pastas vão sendo preenchidas ao longo do cronograma.

## Cronograma

| Semana | Data | Trabalho | Tema |
|---|---|---|---|
| S8 | 04/11 | Enunciado; imagens dos 4 serviços e Compose | T02 |
| S9 | 11/11 | Kubernetes: Deployments, Ingress, Postgres, SQL Server | T03 |
| S10 | 18/11 | Timeouts, retries, idempotência, RabbitMQ · E2, E5 | T04 |
| S11 | 25/11 | UPDATE condicional, réplica, k6 · E1, E6 | T05 |
| S12 | 02/12 | Prometheus, Grafana, SLO, HPA, sondas, exporter SQL · E3, E4, E8 | T06 |
| S13 | 09/12 | NetworkPolicy, Secrets, RBAC · E7; fecho e ensaio | T06 |
| S14 | 16/12 | Defesa: demonstração ao vivo | — |

## Ambiente

Windows 11 + WSL2 Ubuntu + Docker Desktop, kubectl, k3d (k3s), Helm, k6. Todos os comandos correm no terminal Ubuntu.
