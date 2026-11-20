package com.kafka.recomendaciones.Service;

import com.kafka.recomendaciones.Model.Interacciones;
import com.kafka.recomendaciones.Model.Producto;
import com.kafka.recomendaciones.Model.Recomendaciones;
import com.kafka.recomendaciones.Repository.InteraccionesRepository;
import com.kafka.recomendaciones.Repository.RecomendacionesRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Motor de Cálculo de Recomendaciones y Métricas Analíticas en Tiempo Real.
 * Implementa la ponderación de acciones de la pauta docente:
 * - Compra (purchase) -> Peso 3
 * - Carrito (cart) -> Peso 2
 * - Vista (view) -> Peso 1
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class RecomendacionesService {

    private final InteraccionesRepository interaccionesRepository;
    private final RecomendacionesRepository recomendacionesRepository;
    private final WebSocketNotificationService webSocketNotificationService;

    // Catálogo maestro sincronizado de la tienda Pedidos360
    private static final List<Producto> CATALOGO_MAESTRO = List.of(
            new Producto("PROD-101", "Laptop Asus ROG Strix G16", "Tecnología", "Computación", "Asus", 1290000, 4.9, "https://images.unsplash.com/photo-1603302576837-37561b2e2302?auto=format&fit=crop&w=600&q=80", 15),
            new Producto("PROD-001", "Teclado Mecánico RGB Corsair K70", "Accesorios", "Periféricos", "Corsair", 119990, 4.8, "https://images.unsplash.com/photo-1588872657578-7efd1f1555ed?auto=format&fit=crop&w=600&q=80", 25),
            new Producto("PROD-102", "Monitor Gamer Samsung Odyssey G7 28\"", "Monitores", "Pantallas", "Samsung", 489990, 4.7, "https://images.unsplash.com/photo-1517336714731-489689fd1ca8?auto=format&fit=crop&w=600&q=80", 8),
            new Producto("PROD-202", "Mouse Inalámbrico Logitech G Pro X Superlight", "Accesorios", "Periféricos", "Logitech", 109990, 4.9, "https://images.unsplash.com/photo-1615663245857-ac93bb7c39e7?auto=format&fit=crop&w=600&q=80", 30),
            new Producto("PROD-203", "Audífonos Gamer HyperX Cloud II Wireless", "Audio", "Sonido", "HyperX", 139990, 4.6, "https://images.unsplash.com/photo-1587829741301-dc798b83add3?auto=format&fit=crop&w=600&q=80", 18),
            new Producto("PROD-301", "Silla Ergonómica Gamer Secretlab Titan", "Mobiliario", "Confort", "Secretlab", 399990, 4.8, "https://images.unsplash.com/photo-1527443224154-c4a3942d3acf?auto=format&fit=crop&w=600&q=80", 5),
            new Producto("PROD-005", "Tarjeta Gráfica NVIDIA RTX 4070 Ti Super", "Componentes", "Hardware", "NVIDIA", 849990, 5.0, "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=600&q=80", 4)
    );

    // Contadores en memoria para telemetría WebSocket
    private final AtomicInteger contadorInteracciones = new AtomicInteger(0);
    private final Map<String, Integer> popularidadProductos = new ConcurrentHashMap<>();
    private final List<Map<String, Object>> historialAlertas = Collections.synchronizedList(new ArrayList<>());

    /**
     * Recalcula y persiste el Top 5 de productos recomendados para un usuario.
     */
    public void calcularYGuardarTop5(String userId) {
        if (userId == null || userId.trim().isEmpty()) return;

        List<Interacciones> interacciones = interaccionesRepository.findByUserId(userId);
        if (interacciones.isEmpty()) return;

        // Sumar pesos ponderados por producto
        Map<String, Double> scoreMap = interacciones.stream()
                .collect(Collectors.groupingBy(
                        Interacciones::getItemId,
                        Collectors.summingDouble(i -> (double) i.getWeight())
                ));

        List<Map.Entry<String, Double>> topEntries = scoreMap.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .limit(5)
                .toList();

        recomendacionesRepository.deleteByUserId(userId);

        int rank = 1;
        for (Map.Entry<String, Double> entry : topEntries) {
            String itemId = entry.getKey();
            Double score = entry.getValue();

            String categoria = interacciones.stream()
                    .filter(i -> i.getItemId().equals(itemId))
                    .map(Interacciones::getCategory)
                    .filter(Objects::nonNull)
                    .findFirst().orElse("Tecnología");

            String productType = interacciones.stream()
                    .filter(i -> i.getItemId().equals(itemId))
                    .map(Interacciones::getProductType)
                    .filter(Objects::nonNull)
                    .findFirst().orElse("Dispositivo");

            Recomendaciones rec = new Recomendaciones(userId, itemId, score, rank++, categoria, productType);
            recomendacionesRepository.save(rec);
        }

        // Actualizar popularidad global
        topEntries.forEach(entry ->
                popularidadProductos.merge(entry.getKey(), entry.getValue().intValue(), Integer::sum));

        contadorInteracciones.incrementAndGet();

        // Emitir actualización por WebSocket
        transmitirMetricasActuales();
    }

    /**
     * Retorna los productos recomendados para un usuario, complementando con catálogo si es nuevo.
     */
    public List<Producto> obtenerRecomendacionesParaUsuario(String userId) {
        List<Recomendaciones> topRecs = recomendacionesRepository.findByUserIdOrderByRankPositionAsc(userId);

        if (topRecs.isEmpty()) {
            // Usuario sin historial previo: retorna los 3 productos más destacados del catálogo
            return CATALOGO_MAESTRO.stream().limit(3).toList();
        }

        List<Producto> resultado = new ArrayList<>();
        for (Recomendaciones rec : topRecs) {
            CATALOGO_MAESTRO.stream()
                    .filter(p -> p.getId().equalsIgnoreCase(rec.getItemId()))
                    .findFirst()
                    .ifPresent(resultado::add);
        }

        // Si faltan para completar 3, rellenar con productos populares
        if (resultado.size() < 3) {
            CATALOGO_MAESTRO.stream()
                    .filter(p -> resultado.stream().noneMatch(r -> r.getId().equals(p.getId())))
                    .limit(3 - resultado.size())
                    .forEach(resultado::add);
        }

        return resultado;
    }

    /**
     * Registra una alerta antifraude para mostrar en el feed en vivo
     */
    public void registrarAlertaFraude(String clientId, String razon) {
        Map<String, Object> alerta = Map.of(
                "tipo", "FRAUDE_DETECTADO",
                "clientId", clientId,
                "razon", razon,
                "timestamp", System.currentTimeMillis()
        );
        historialAlertas.add(0, alerta);
        if (historialAlertas.size() > 20) historialAlertas.remove(historialAlertas.size() - 1);
        transmitirMetricasActuales();
    }

    /**
     * Construye y difunde el payload con las 3 métricas en tiempo real requeridas por la rúbrica.
     */
    public void transmitirMetricasActuales() {
        // Métrica 1: Volumen total de interacciones procesadas
        int totalClicks = contadorInteracciones.get();

        // Métrica 2: Top 5 productos más demandados
        List<Map<String, Object>> top5 = popularidadProductos.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .limit(5)
                .map(entry -> {
                    String nombre = CATALOGO_MAESTRO.stream()
                            .filter(p -> p.getId().equalsIgnoreCase(entry.getKey()))
                            .map(Producto::getNombre)
                            .findFirst().orElse(entry.getKey());
                    return Map.<String, Object>of(
                            "itemId", entry.getKey(),
                            "nombre", nombre,
                            "puntosInteraccion", entry.getValue()
                    );
                })
                .toList();

        // Métrica 3: Feed de seguridad y alertas
        List<Map<String, Object>> ultimasAlertas = new ArrayList<>(historialAlertas.stream().limit(5).toList());

        Map<String, Object> payload = Map.of(
                "metrica1_totalInteracciones", totalClicks,
                "metrica2_topProductos", top5,
                "metrica3_alertasSeguridad", ultimasAlertas,
                "timestamp", System.currentTimeMillis()
        );

        webSocketNotificationService.emitirMetricasEnVivo(payload);
    }
}
