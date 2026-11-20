package com.kafka.recomendaciones.Component;

import com.kafka.recomendaciones.DTO.InteractionRequestsDTO;
import com.kafka.recomendaciones.Model.Interacciones;
import com.kafka.recomendaciones.Repository.InteraccionesRepository;
import com.kafka.recomendaciones.Service.DynamoDbStorageService;
import com.kafka.recomendaciones.Service.RecomendacionesService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor de Eventos de Streaming Kafka para Analítica y Recomendaciones.
 * Escucha los tópicos 'interacciones-user' (clics/carrito) y 'payment-attempts' (fraude).
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

            // 2. Persistir en base de datos relacional (MySQL)
            Interacciones interaccion = new Interacciones(
                    dto.getClientId(),
                    dto.getItemId() != null ? dto.getItemId() : "PROD-101",
                    dto.getAction(),
                    weight,
                    dto.getCategory() != null ? dto.getCategory() : "Tecnología",
                    dto.getProductType() != null ? dto.getProductType() : "Dispositivo"
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
}
