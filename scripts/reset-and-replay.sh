#!/bin/sh
# Reconstrucción de vistas: detener el proyector, borrar únicamente sus proyecciones y reiniciar con grupo de replay.
docker compose stop proyector
docker compose exec -T mongo mongosh rutasur --quiet --eval 'db.envios.deleteMany({}); db.alertas.deleteMany({}); db.notificaciones.deleteMany({}); db.eventos_procesados.deleteMany({consumidor:"proyector-replay"})'
docker compose run -d --name rutasur-proyector-replay --no-deps -e ROLE=proyector-replay proyector node services/worker.js proyector-replay
