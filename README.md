# RutaSur — TP7 Programación Avanzada (2026)

Implementación local del caso práctico de logística orientada a eventos. Seis topics Kafka, simulador con cinco escenarios, cinco servicios Node.js independientes, vistas MongoDB, y cinco pantallas Next.js. Las extensiones opcionales del enunciado no forman parte del proyecto.

## Requisitos y arranque

- Docker Desktop con Docker Compose y al menos 6 GB de RAM disponibles.
- En la carpeta raíz ejecutar **`docker compose up --build -d`**.
- Abrir **http://localhost:3000**: Torre de control. Panel de demo: **http://localhost:3000/demo**.
- El primer arranque descarga imágenes y compila Next.js. Los servicios arrancan después de inicializar Kafka y el replica set MongoDB.
- Inspeccionar logs: `docker compose logs -f simulador proyector monitor-sla`.
- Detener: `docker compose down` (preserva datos). Reiniciar limpio: `docker compose down -v` (elimina datos).

## Arquitectura

`simulador → Kafka → (proyector | notificaciones | reprogramacion | monitor-sla | analitica) → Kafka/MongoDB → Next.js`.

Los procesos no llaman directamente a otros consumidores; sus interacciones de datos son mediante Kafka. El panel de demostración envía órdenes al simulador mediante su endpoint de control, no consulta Kafka. Next.js consulta MongoDB y se actualiza mediante MongoDB change streams + SSE.

| Topic | Clave | Productor | Consumidores |
| --- | --- | --- | --- |
| `envios.estados` | shipmentId | Simulador | Todos los consumidores |
| `envios.promesas` | shipmentId | Simulador | Proyector, monitor |
| `envios.reprogramaciones` | shipmentId | Reprogramación | Proyector |
| `alertas.sla` | shipmentId | Monitor | Proyector, notificaciones |
| `notificaciones.enviadas` | shipmentId | Notificaciones | Proyector |
| `envios.estados.dlq` | shipmentId | Consumidores | Inspección manual |

Cada servicio usa su propio consumer group. Entrega *at least once*, commit manual del offset luego del manejo, marcador `(consumidor,eventId)` en MongoDB con índice único y transacciones para efectos idempotentes; una secuencia menor o igual no se vuelve a proyectar. En `envios.estados` un evento inválido se publica en DLQ con el motivo. MongoDB funciona con replica set mononodo para transacciones y change streams.

## Verificación de la demo

1. Abrir torre de control y observar aumento de envíos por el flujo automático.
2. Abrir `Panel de demo → Envío feliz`, seguir el enlace al detalle, y abrir `/tracking/<shipmentId>` para verificar el tracking público.
3. `Destinatario ausente`: ver aviso, decisión de reprogramación y analítica de fallos.
4. `Demora en hub`: observar alertas de prioridad según el tiempo virtual.
5. `docker compose stop notificaciones` y disparar escenarios `ausente`; los otros consumidores siguen. `docker compose start notificaciones`: consume pendientes.
6. Inyectar un evento inválido desde el simulador automático (1%): revisar `docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic envios.estados.dlq --from-beginning --max-messages 1`.
7. Abrir analítica y filtrar por zona, hub y fecha.
8. Calcular cuarto indicador con `docker compose run --rm --no-deps proyector node scripts/replay.js` y volver a analítica. Para detener el consumidor nuevo tras completar el histórico, usar Ctrl+C.

## Inspección de Kafka / MongoDB

- Grupos: `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list`
- Lag: `docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group notificaciones`
- Vistas: `docker compose exec mongo mongosh rutasur --eval 'db.envios.findOne()'`

## Reconstrucción de la vista `envios`

Un grupo nuevo de proyección debe consumir desde offset 0. Después de detener `proyector` ejecutar:

```sh
docker compose stop proyector
docker compose exec mongo mongosh rutasur --eval 'db.envios.deleteMany({})'
docker compose run --rm --no-deps proyector node services/worker.js proyector-replay
```

En una segunda terminal comparar resultados. El replay lee desde el principio por ser un grupo nuevo. Para volver a modo habitual, Ctrl+C, `docker compose start proyector`. Cuidado: la proyección no restaura necesariamente el mismo instante de la simulación si llegaron nuevos eventos entre ambas consultas.

## Parámetros del simulador

Usar `SIM_RATE`, `SIM_SPEED`, `SIM_SEED` en un archivo `.env` opcional, de acuerdo con `.env.example`. Valores por defecto: 20 envíos/minuto, velocidad 120x y semilla 2026. Situaciones: `feliz`, `ausente`, `demorado`, `tercer-fallo` y `rafaga` (500). Distribución de fallos por zona: 5 %, 18 % y 30 %; razones de fallo: 60 % ausente, 20 % dirección, 15 % rechazo, 5 % zona. Se inyecta un 2 % de duplicados, 2 % fuera de orden y 1 % inválidos aproximadamente.

## Notas técnicas

- MongoDB registra vistas derivadas; Kafka retiene la fuente de eventos.
- Los límites de 2 segundos y la recuperación se verifican en ejecución real, no están medidos en este archivo.
- Es una implementación de laboratorio sin integración con correo, SMS, GPS, sistemas externos o autenticación, según el alcance del TP.
