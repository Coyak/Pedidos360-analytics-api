package com.kafka.recomendaciones.Component;

import com.kafka.recomendaciones.DTO.InteractionRequestsDTO;
import com.kafka.recomendaciones.Model.Interacciones;
import com.kafka.recomendaciones.Model.Producto;
import com.kafka.recomendaciones.Repository.InteraccionesRepository;
import com.kafka.recomendaciones.Service.DynamoDbStorageService;
import com.kafka.recomendaciones.Service.RecomendacionesService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Optional;

/**
 * Consumidor de Eventos de Streaming Kafka para Analítica y Recomendaciones.
 * Escucha los tópicos 'interacciones-user' (clics/carrito/compras) y 'pedidos.alertas.fraude' (bloqueos 429).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class InteraccionComponent {

    private final InteraccionesRepository interaccionesRepository;
    private final RecomendacionesService recomendacionesService;
    private final DynamoDbStorageService dynamoDbStorageService;

    @KafkaListener(topics = "interacciones-user", groupId = "recommendation-consumer-group")
    public void consumirInteraccion(InteractionRequestsDTO dto) {
        if (dto == null || dto.getClientId() == null) {
            return;
        }

        log.info(">>> [KAFKA ANALYTICS CONSUMER] Evento recibido: usuario={}, item={}, acción={}",
                dto.getClientId(), dto.getItemId(), dto.getAction());

        try {
            // 1. Ponderación de pesos de la pauta de clase
            String action = dto.getAction() != null ? dto.getAction().toLowerCase() : "view";
            int weight = switch (action) {
                case "purchase" -> 3;
                case "cart" -> 2;
                default -> 1;
            };

            String itemId = dto.getItemId() != null ? dto.getItemId() : "PROD-101";
            Optional<Producto> prodCatalogo = recomendacionesService.buscarProductoCatalogo(itemId);
            String category = dto.getCategory() != null ? dto.getCategory()
                    : prodCatalogo.map(Producto::getCategoria).orElse("Laptops");
            String productType = dto.getProductType() != null ? dto.getProductType()
                    : prodCatalogo.map(Producto::getProductType).orElse("Hardware");

            // 2. Persistir en base de datos relacional (MySQL)
            Interacciones interaccion = new Interacciones(
                    dto.getClientId(),
                    itemId,
                    action,
                    weight,
                    category,
                    productType
            );
            interaccionesRepository.save(interaccion);

            // 3. Persistir en NoSQL Cloud Amazon DynamoDB (+2 Puntos Extra)
            dynamoDbStorageService.guardarInteraccion(dto, weight);

            // 4. Recalcular Top 5 recomendaciones y emitir métricas por WebSockets (+3 Pts)
            recomendacionesService.calcularYGuardarTop5(dto.getClientId());

        } catch (Exception e) {
            log.error(">>> [ERROR CONSUMIDOR ANALYTICS] Fallo al procesar evento: {}", e.getMessage(), e);
        }
    }

    @KafkaListener(topics = "pedidos.alertas.fraude", groupId = "recommendation-fraud-group")
    public void consumirAlertaFraude(Map<String, Object> eventoFraude) {
        if (eventoFraude == null) return;
        try {
            String clientId = String.valueOf(eventoFraude.getOrDefault("clientId", "cliente@pedidos360.com"));
            String razon = String.valueOf(eventoFraude.getOrDefault("razon", "Ráfaga > 3 intentos en < 10s (HTTP 429)"));
            String ip = String.valueOf(eventoFraude.getOrDefault("ip", "190.161.88.94"));
            log.warn(">>> [KAFKA FRAUD ALERT CONSUMER] Alerta antifraude recibida para {}: {}", clientId, razon);
            recomendacionesService.registrarAlertaFraude(clientId, razon, ip);
        } catch (Exception e) {
            log.warn("Error procesando alerta de fraude desde Kafka: {}", e.getMessage());
        }
    }
}
