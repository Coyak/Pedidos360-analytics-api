# Pedidos360 - Microservicio de Analítica, Kafka & Recomendaciones (ms-analytics)

Microservicio de analítica orientada a eventos desarrollado en **Spring Boot 3.4 (Java 21)** para la **Evaluación 3** de Desarrollo Cloud. Implementa ingesta de eventos de usuario a través de **Apache Kafka (KRaft)**, motor de recomendaciones personalizadas por afinidad multi-categoría, persistencia NoSQL opcional en **AWS DynamoDB (+2 pts)** y telemetría de métricas en tiempo real por **WebSockets (+3 pts)**.

---

## 🎯 Alcance y Funcionalidades Principales

1. **Ingesta de Eventos con Apache Kafka (≥2 Tópicos Activos):**
   * `topic-interacciones`: Recibe eventos ponderados de usuario (`view=1`, `cart=2`, `purchase=3`) con `itemId`, `category` y `clientId`.
   * `topic-alertas-fraude`: Recibe eventos de bloqueos preventivos `HTTP 429` generados por `ms-pago`.
   * `topic-metricas`: Streaming continuo de KPIs transaccionales y de interacción.
2. **Motor de Recomendaciones Inteligente Multi-Categoría:**
   * Analiza el historial acumulado del cliente y calcula la afinidad por categoría.
   * Genera recomendaciones dinámicas diversificadas en las 5 categorías oficiales: `Laptops`, `Periféricos`, `Monitores`, `Audio` y `Almacenamiento`.
   * En caso de usuario nuevo (cold-start), provee un catálogo balanceado de productos destacados.
3. **Persistencia NoSQL en AWS DynamoDB (+2 Puntos Extra EV3):**
   * Integración nativa con `software.amazon.awssdk:dynamodb` (SDK v2).
   * Almacena cada interacción en la tabla `Pedidos360_Interacciones` con clave particionada `userId` (HASH) y `timestamp_eventId` (RANGE).
   * Respaldo híbrido transparente con MySQL 8.0 (`pagos_db`) ante indisponibilidad de credenciales en nube.
4. **Streaming en Tiempo Real con WebSockets (+3 Puntos Extra EV3):**
   * Endpoint dual: WebSocket directo (`/ws-analytics-raw`) y STOMP (`/ws-analytics` -> `/topic/metricas`).
   * Emite en vivo: total de interacciones, ranking Top 5 de productos más demandados, tasa de conversión y feed de eventos.
5. **Cumplimiento Estricto "CERO LOCALHOST":**
   * Parámetros remotos configurables vía `SPRING_KAFKA_BOOTSTRAP_SERVERS`, `SPRING_DATASOURCE_URL`, `AWS_REGION`, `DYNAMODB_TABLE_NAME`.

---

## 📡 Endpoints de la API (Puerto 8086)

| Método | Endpoint | Descripción | Respuestas |
|---|---|---|---|
| `GET` | `/api/recomendaciones/status` | Healthcheck del servicio y estado de Kafka/DynamoDB | `200 OK` (JSON) |
| `POST` | `/api/recomendaciones/interaccion` | Registra evento de usuario (`view`, `cart`, `purchase`) | `200 OK` |
| `GET` | `/api/recomendaciones?userId={email}` | Obtiene las recomendaciones personalizadas del usuario | `200 OK` (Array JSON) |
| `GET` | `/api/recomendaciones/metricas` | KPIs consolidados (Top 5, interacciones, fraudes) | `200 OK` (JSON) |
| `POST` | `/api/recomendaciones/alerta-fraude` | Ingesta alertas de seguridad desde ms-pago | `200 OK` |
| `WS` | `/ws-analytics-raw` | Canal WebSocket directo de telemetría en tiempo real | Conexión duplex |

---

## 🐳 Despliegue con Docker

El servicio incluye un `Dockerfile` multi-stage para compilación autónoma:
```bash
docker build -t pedidos360-analytics-api .
docker run -d --name pedidos360-analytics-api -p 8086:8086 pedidos360-analytics-api
```
