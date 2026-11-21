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
import java.util.concurrent.TimeUnit;

@Slf4j
@RestController
@RequestMapping({"/api/recomendaciones", "/api"})
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
     * Endpoint oficial por PathVariable: GET /api/recomendaciones/{userId}
     */
    @GetMapping("/{userId}")
    public ResponseEntity<?> obtenerRecomendaciones(@PathVariable String userId) {
        if ("status".equalsIgnoreCase(userId)) {
            return getStatus();
        }
        if ("metricas".equalsIgnoreCase(userId)) {
            return obtenerMetricas();
        }
        log.info(">>> [GET /api/recomendaciones/{}] Solicitando recomendaciones personalizadas", userId);
        List<Producto> recomendados = recomendacionesService.obtenerRecomendacionesParaUsuario(userId);
        return ResponseEntity.ok(recomendados);
    }

    /**
     * Soporte por Query Param: GET /api/recomendaciones?userId=...
     */
    @GetMapping
    public ResponseEntity<List<Producto>> obtenerRecomendacionesQuery(
            @RequestParam(value = "userId", defaultValue = "cliente@pedidos360.com") String userId) {
        log.info(">>> [GET /api/recomendaciones?userId={}] Solicitando recomendaciones personalizadas", userId);
        List<Producto> recomendados = recomendacionesService.obtenerRecomendacionesParaUsuario(userId);
        return ResponseEntity.ok(recomendados);
    }

    /**
     * Endpoint para registrar interacciones de usuario (view, cart, purchase) y publicarlas a Kafka.
     * Soporta tanto POST /api/recomendaciones/interaccion como POST /api/interacciones.
     */
    @PostMapping({"/interaccion", "/interacciones"})
    public ResponseEntity<?> registrarInteraccion(@RequestBody InteractionRequestsDTO request) {
        if (request.getClientId() == null || request.getItemId() == null) {
            return ResponseEntity.badRequest().body(Map.of("error", "clientId e itemId son obligatorios"));
        }
        if (request.getTimestamp() == null) {
            request.setTimestamp(System.currentTimeMillis());
        }

        try {
            kafkaTemplate.send("interacciones-user", request.getClientId(), request).get(2, TimeUnit.SECONDS);
            log.info(">>> [KAFKA DISPATCH] Interacción publicada al tópico 'interacciones-user': {} -> {}",
                    request.getClientId(), request.getItemId());
            return ResponseEntity.ok(Map.of("status", "ENVIADO_A_KAFKA", "data", request));
        } catch (Exception e) {
            log.warn("Broker Kafka no disponible de inmediato ({}). Persistiendo y recalculando directamente.", e.getMessage());
            recomendacionesService.procesarInteraccionDirecta(request);
            return ResponseEntity.ok(Map.of("status", "PROCESADO_LOCALMENTE", "data", request));
        }
    }

    /**
     * Consulta el snapshot actual de métricas analíticas (Top 5, totales, eventos y alertas antifraude)
     */
    @GetMapping("/metricas")
    public ResponseEntity<?> obtenerMetricas() {
        return ResponseEntity.ok(recomendacionesService.obtenerSnapshotMetricas());
    }

    /**
     * Registra una alerta antifraude (HTTP 429) y emite actualización en vivo por WebSocket
     */
    @PostMapping("/alerta-fraude")
    public ResponseEntity<?> registrarAlertaFraude(@RequestBody Map<String, String> body) {
        String clientId = body != null ? body.getOrDefault("clientId", "cliente.bloqueado@pedidos360.com") : "cliente.bloqueado@pedidos360.com";
        String razon = body != null ? body.getOrDefault("razon", "Ráfaga > 3 intentos en < 10 segundos (HTTP 429)") : "Ráfaga > 3 intentos en < 10 segundos (HTTP 429)";
        String ip = body != null ? body.getOrDefault("ip", "190.161.42.12") : "190.161.42.12";
        recomendacionesService.registrarAlertaFraude(clientId, razon, ip);
        return ResponseEntity.ok(Map.of("status", "ALERTA_REGISTRADA", "clientId", clientId));
    }

    /**
     * Dispara la emisión forzada de telemetría a través del canal WebSocket
     */
    @PostMapping("/emitir-metricas")
    public ResponseEntity<?> forzarEmision() {
        Map<String, Object> snapshot = recomendacionesService.transmitirMetricasActuales();
        return ResponseEntity.ok(Map.of("status", "EMITIDO", "metricas", snapshot));
    }
}
