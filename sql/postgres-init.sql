-- Corre uma vez, quando o volume do Postgres é criado.
-- Uma base de dados por serviço: nenhum serviço lê as tabelas de outro.
CREATE DATABASE agenda;
CREATE DATABASE marcacoes;
CREATE DATABASE notificacoes;
