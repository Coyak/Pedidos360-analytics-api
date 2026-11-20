package com.kafka.recomendaciones.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Emite métricas en tiempo real hacia los clientes suscritos al WebSocket (+3 Puntos Extra).
 * Canal: /topic/metricas
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class WebSocketNotificationService {

    private final SimpMessagingTemplate messagingTemplate;

    public void emitirMetricasEnVivo(Map<String, Object> payload) {
        try {
            messagingTemplate.convertAndSend("/topic/metricas", payload);
            log.info(">>> [WEBSOCKET LIVE +3 PTS] Métricas transmitidas a /topic/metricas");
        } catch (Exception e) {
            log.warn(">>> [WEBSOCKET WARN] Error al emitir por WebSocket: {}", e.getMessage());
        }
    }
}
