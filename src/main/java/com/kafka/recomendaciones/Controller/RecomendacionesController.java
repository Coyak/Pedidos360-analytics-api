package com.kafka.recomendaciones.Controller;

import com.kafka.recomendaciones.DTO.InteractionRequestsDTO;
import com.kafka.recomendaciones.Model.Producto;
import com.kafka.recomendaciones.Service.RecomendacionesService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/recomendaciones")
@CrossOrigin(origins = "*")
@RequiredArgsConstructor
public class RecomendacionesController {

    private final RecomendacionesService recomendacionesService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @GetMapping("/status")
    public ResponseEntity<?> getStatus() {
        return ResponseEntity.ok(Map.of(
                "service", "Pedidos360-analytics-api",
                "status", "UP",
                "kafkaGroup", "recommendation-consumer-group",
                "features", List.of("Kafka Consumer", "Top 5 Recommendations", "Amazon DynamoDB NoSQL (+2 pts)", "WebSockets Live Metrics (+3 pts)"),
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Endpoint oficial requerido: Retorna los productos sugeridos para el cliente en base a sus eventos de Kafka
     */
    @GetMapping("/{userId}")
    public ResponseEntity<List<Producto>> obtenerRecomendaciones(@PathVariable String userId) {
        log.info(">>> [GET /api/recomendaciones/{}] Solicitando recomendaciones personalizadas", userId);
        List<Producto> recomendados = recomendacionesService.obtenerRecomendacionesParaUsuario(userId);
        return ResponseEntity.ok(recomendados);
    }

    /**
     * Endpoint para simular o recibir interacciones directas y publicarlas a Kafka
     */
    @PostMapping("/interaccion")
    public ResponseEntity<?> registrarInteraccion(@RequestBody InteractionRequestsDTO request) {
        if (request.getClientId() == null || request.getItemId() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "clientId e itemId son obligatorios"));
        }
        if (request.getTimestamp() == null) {
            request.setTimestamp(System.currentTimeMillis());
        }

        try {
            kafkaTemplate.send("interacciones-user", request.getClientId(), request);
            log.info(">>> [KAFKA DISPATCH] Interacción publicada al tópico 'interacciones-user'");
            return ResponseEntity.ok(Map.of("status", "ENVIADO_A_KAFKA", "data", request));
        } catch (Exception e) {
            log.warn("Fallo al enviar a Kafka ({}). Procesando localmente.", e.getMessage());
            recomendacionesService.calcularYGuardarTop5(request.getClientId());
            return ResponseEntity.ok(Map.of("status", "PROCESADO_LOCALMENTE", "data", request));
        }
    }

    /**
     * Dispara la emisión forzada de telemetría a través del canal WebSocket
     */
    @PostMapping("/emitir-metricas")
    public ResponseEntity<?> forzarEmision() {
        recomendacionesService.transmitirMetricasActuales();
        return ResponseEntity.ok(Map.of("status", "EMITIDO"));
    }
}
