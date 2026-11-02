#!/usr/bin/env bash
# Teste de ponta a ponta da VagaCerta em Docker Compose.
#   bash testes/e2e.sh            sobe o Compose (com build), corre os testes e grava resultados/e2e-AAAAMMDD-HHMM.txt
#   LIMPAR=1 bash testes/e2e.sh   começa de volumes vazios (docker compose down -v)
# O teste corre num contentor node:22-alpine ligado à rede do Compose; não é preciso Node no Ubuntu.
set -uo pipefail
cd "$(dirname "$0")/.."

relatorio="resultados/e2e-$(date +%Y%m%d-%H%M).txt"
mkdir -p resultados
exec > >(tee "$relatorio") 2>&1

node_teste() {
  docker run --rm --network vagacerta_default \
    -v "$PWD/testes:/testes:ro" -v "$PWD/resultados:/resultados" -u "$(id -u):$(id -g)" \
    node:22-alpine node /testes/e2e.mjs "$@"
}

echo "VagaCerta: teste de ponta a ponta  $(date '+%Y-%m-%d %H:%M')  ($(git rev-parse --short HEAD 2>/dev/null || echo sem-git))"
[ "${LIMPAR:-0}" = 1 ] && docker compose down -v --remove-orphans

echo -e "\n== Build e arranque"
t0=$SECONDS
if ! docker compose up -d --build --wait --wait-timeout 300; then
  echo "o Compose não ficou saudável:"; docker compose ps; docker compose logs --tail 30; exit 1
fi
echo "pronto em $((SECONDS - t0)) s"
docker compose ps --format 'table {{.Service}}\t{{.Image}}\t{{.Status}}'

falhas=0
echo -e "\n== Teste funcional"
node_teste funcional || falhas=$((falhas + 1))

echo -e "\n== Resiliência: RabbitMQ parado"
docker compose stop rabbitmq >/dev/null 2>&1 && echo "rabbitmq parado"
node_teste sem-rabbit || falhas=$((falhas + 1))
docker compose start rabbitmq >/dev/null 2>&1 && echo "rabbitmq a arrancar"
node_teste sem-rabbit-verifica || falhas=$((falhas + 1))

echo -e "\n== Erros nos registos dos serviços"
for s in agenda marcacoes utentes notificacoes; do
  n=$(docker compose logs --no-log-prefix "$s" 2>/dev/null | grep -c '"level":"error"')
  echo "  $s: $n"
done

rm -f resultados/.sem-rabbit resultados/.e2e-*.json
echo -e "\n== Resultado: $([ $falhas = 0 ] && echo 'TODAS AS FASES PASSARAM' || echo "$falhas FASE(S) COM FALHAS")"
echo "relatório: $relatorio"
exit $falhas
