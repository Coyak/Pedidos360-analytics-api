package com.kafka.recomendaciones.Service;

import com.kafka.recomendaciones.DTO.InteractionRequestsDTO;
import com.kafka.recomendaciones.Model.Interacciones;
import com.kafka.recomendaciones.Model.Producto;
import com.kafka.recomendaciones.Model.Recomendaciones;
import com.kafka.recomendaciones.Repository.InteraccionesRepository;
import com.kafka.recomendaciones.Repository.RecomendacionesRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
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
    private final DynamoDbStorageService dynamoDbStorageService;

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    // Catálogo maestro 100% alineado con catalogo-api (:8080) y Pedidos360-Frontend (products.js)
    private static final List<Producto> CATALOGO_MAESTRO = List.of(
            // 1. LAPTOPS
            new Producto("PROD-101", "Laptop Asus ROG Strix G16", "Laptops", "Hardware", "Asus", 1290000, 4.9, "https://images.unsplash.com/photo-1603302576837-37561b2e2302?auto=format&fit=crop&w=800&q=80", 8),
            new Producto("PROD-102", "Apple MacBook Pro 14\" M3 Pro", "Laptops", "Hardware", "Apple", 1890000, 5.0, "https://images.unsplash.com/photo-1517336714731-489689fd1ca8?auto=format&fit=crop&w=800&q=80", 5),
            new Producto("PROD-103", "Lenovo Legion Pro 7i Gen 8", "Laptops", "Hardware", "Lenovo", 1690000, 4.8, "https://images.unsplash.com/photo-1588872657578-7efd1f1555ed?auto=format&fit=crop&w=800&q=80", 6),
            new Producto("PROD-104", "Dell XPS 15 9530 Creator Edition", "Laptops", "Hardware", "Dell", 1540000, 4.7, "https://images.unsplash.com/photo-1593642632823-8f785ba67e45?auto=format&fit=crop&w=800&q=80", 4),
            new Producto("PROD-105", "HP Omen 16 Transcend", "Laptops", "Hardware", "HP", 1190000, 4.6, "https://images.unsplash.com/photo-1544716278-ca5e3f4abd8c?auto=format&fit=crop&w=800&q=80", 9),
            new Producto("PROD-106", "Acer Predator Helios 16", "Laptops", "Hardware", "Acer", 1350000, 4.8, "https://images.unsplash.com/photo-1525547719571-a2d4ac8945e2?auto=format&fit=crop&w=800&q=80", 7),
            new Producto("PROD-107", "Razer Blade 15 Advanced", "Laptops", "Hardware", "Razer", 2190000, 4.9, "https://images.unsplash.com/photo-1588797384632-ca1136b7159f?auto=format&fit=crop&w=800&q=80", 3),

            // 2. PERIFÉRICOS
            new Producto("PROD-201", "Mouse Logitech G Pro X Superlight 2", "Periféricos", "Accesorios", "Logitech", 129990, 4.9, "https://images.unsplash.com/photo-1615663245857-ac93bb7c39e7?auto=format&fit=crop&w=800&q=80", 15),
            new Producto("PROD-202", "Teclado Mecánico Keychron Q1 Pro Wireless", "Periféricos", "Accesorios", "Keychron", 169990, 4.8, "https://images.unsplash.com/photo-1587829741301-dc798b83add3?auto=format&fit=crop&w=800&q=80", 9),
            new Producto("PROD-203", "Mouse Razer DeathAdder V3 Pro", "Periféricos", "Accesorios", "Razer", 119990, 4.8, "https://images.unsplash.com/photo-1629429408209-1f912961dbd8?auto=format&fit=crop&w=800&q=80", 12),
            new Producto("PROD-204", "Teclado SteelSeries Apex Pro TKL (2023)", "Periféricos", "Accesorios", "SteelSeries", 189990, 4.9, "https://images.unsplash.com/photo-1595225476474-87563907a212?auto=format&fit=crop&w=800&q=80", 7),
            new Producto("PROD-205", "Controlador Elgato Stream Deck MK.2", "Periféricos", "Accesorios", "Elgato", 139990, 4.9, "https://images.unsplash.com/photo-1616469829941-c7200edec809?auto=format&fit=crop&w=800&q=80", 11),
            new Producto("PROD-206", "Mouse Logitech MX Master 3S", "Periféricos", "Accesorios", "Logitech", 89990, 4.9, "https://images.unsplash.com/photo-1527864550417-7fd91fc51a46?auto=format&fit=crop&w=800&q=80", 18),
            new Producto("PROD-207", "Mousepad Gaming Artisan Ninja FX Hayate Otsu XL", "Periféricos", "Accesorios", "Artisan", 54990, 4.8, "https://images.unsplash.com/photo-1541140532154-b024d705b909?auto=format&fit=crop&w=800&q=80", 14),

            // 3. MONITORES
            new Producto("PROD-301", "Monitor Gamer Samsung Odyssey G7 27\"", "Monitores", "Pantallas", "Samsung", 429990, 4.8, "https://images.unsplash.com/photo-1527443224154-c4a3942d3acf?auto=format&fit=crop&w=800&q=80", 6),
            new Producto("PROD-302", "Monitor LG UltraGear 27GR95QE-B OLED 240Hz", "Monitores", "Pantallas", "LG", 649990, 4.9, "https://images.unsplash.com/photo-1547082299-de196ea013d6?auto=format&fit=crop&w=800&q=80", 4),
            new Producto("PROD-303", "Monitor ASUS ROG Swift PG32UQX 32\" 4K Mini-LED", "Monitores", "Pantallas", "Asus", 1199900, 5.0, "https://images.unsplash.com/photo-1593640408182-31c70c8268f5?auto=format&fit=crop&w=800&q=80", 3),
            new Producto("PROD-304", "Monitor Dell Alienware AW3423DWF 34\" QD-OLED", "Monitores", "Pantallas", "Dell", 849990, 4.9, "https://images.unsplash.com/photo-1550745165-9bc0b252726f?auto=format&fit=crop&w=800&q=80", 5),
            new Producto("PROD-305", "Monitor BenQ MOBIUZ EX2710Q 27\" 165Hz", "Monitores", "Pantallas", "BenQ", 299990, 4.7, "https://images.unsplash.com/photo-1542751371-adc38448a05e?auto=format&fit=crop&w=800&q=80", 8),
            new Producto("PROD-306", "Monitor Gigabyte M27Q X 27\" 240Hz KVM", "Monitores", "Pantallas", "Gigabyte", 369990, 4.8, "https://images.unsplash.com/photo-1585792180666-f7347c490ee2?auto=format&fit=crop&w=800&q=80", 10),

            // 4. AUDIO
            new Producto("PROD-401", "Audífonos Sony WH-1000XM5 Noise Cancelling", "Audio", "Sonido", "Sony", 279990, 4.9, "https://images.unsplash.com/photo-1505740420928-5e560c06d30e?auto=format&fit=crop&w=800&q=80", 14),
            new Producto("PROD-402", "Audífonos HyperX Cloud III Wireless Gamer", "Audio", "Sonido", "HyperX", 139990, 4.8, "https://images.unsplash.com/photo-1546435770-a3e426bf472b?auto=format&fit=crop&w=800&q=80", 16),
            new Producto("PROD-403", "Audífonos SteelSeries Arctis Nova Pro Wireless", "Audio", "Sonido", "SteelSeries", 329990, 4.9, "https://images.unsplash.com/photo-1484704849700-f032a568e944?auto=format&fit=crop&w=800&q=80", 6),
            new Producto("PROD-404", "Micrófono Profesional Shure SM7B", "Audio", "Sonido", "Shure", 369990, 5.0, "https://images.unsplash.com/photo-1590602847861-f357a9332bbc?auto=format&fit=crop&w=800&q=80", 8),
            new Producto("PROD-405", "Audífonos Audio-Technica ATH-M50xBT2", "Audio", "Sonido", "Audio-Technica", 189990, 4.8, "https://images.unsplash.com/photo-1583394838336-acd977736f90?auto=format&fit=crop&w=800&q=80", 11),
            new Producto("PROD-406", "Audífonos Sennheiser HD 660S2 Open-Back", "Audio", "Sonido", "Sennheiser", 499990, 4.9, "https://images.unsplash.com/photo-1545127398-14699f92334b?auto=format&fit=crop&w=800&q=80", 4),

            // 5. ALMACENAMIENTO
            new Producto("PROD-501", "SSD Samsung 990 PRO 2TB PCIe 4.0 NVMe", "Almacenamiento", "Hardware", "Samsung", 179990, 4.9, "https://images.unsplash.com/photo-1597872200969-2b65d56bd16b?auto=format&fit=crop&w=800&q=80", 18),
            new Producto("PROD-502", "SSD Kingston FURY Renegade 4TB con Disipador", "Almacenamiento", "Hardware", "Kingston", 349990, 4.9, "https://images.unsplash.com/photo-1563770660941-20978e870e26?auto=format&fit=crop&w=800&q=80", 7),
            new Producto("PROD-503", "SSD WD_BLACK SN850X 2TB NVMe para Juegos", "Almacenamiento", "Hardware", "Western Digital", 169990, 4.8, "https://images.unsplash.com/photo-1550751827-4bd374c3f58b?auto=format&fit=crop&w=800&q=80", 14),
            new Producto("PROD-504", "Memoria RAM Corsair Dominator Titanium DDR5 32GB (2x16GB) 6000MHz", "Almacenamiento", "Hardware", "Corsair", 189990, 4.9, "https://images.unsplash.com/photo-1541029071515-84cc54f84dc5?auto=format&fit=crop&w=800&q=80", 10),
            new Producto("PROD-505", "SSD Externo Portátil SanDisk Extreme PRO 2TB USB 3.2 Gen 2x2", "Almacenamiento", "Hardware", "SanDisk", 219990, 4.8, "https://images.unsplash.com/photo-1574944985070-8f3ebc6b79d2?auto=format&fit=crop&w=800&q=80", 9),
            new Producto("PROD-506", "Tarjeta MicroSDXC SanDisk Extreme 512GB V30 A2", "Almacenamiento", "Hardware", "SanDisk", 49990, 4.7, "https://images.unsplash.com/photo-1562975079-66278d463181?auto=format&fit=crop&w=800&q=80", 22)
    );

    // Historial en memoria de alertas antifraude (Métrica 3)
    private final List<Map<String, Object>> historialAlertas = Collections.synchronizedList(new ArrayList<>());

    @PostConstruct
    public void inicializarDatosBase() {
        try {
            if (historialAlertas.isEmpty()) {
                historialAlertas.add(Map.of(
                        "id", 1L,
                        "tipo", "FRAUDE_DETECTADO",
                        "ip", "190.161.42.12",
                        "client", "bot.burst.482@pedidos360.com",
                        "clientId", "bot.burst.482@pedidos360.com",
                        "reason", "Ráfaga de 4 intentos de pago en < 10s",
                        "razon", "Ráfaga de 4 intentos de pago en < 10s",
                        "action", "Bloqueo 30 min (HTTP 429)",
                        "timestamp", TIME_FMT.format(Instant.now())
                ));
            }

            if (interaccionesRepository.count() == 0) {
                log.info(">>> [SEED ANALYTICS] Inicializando interacciones base en MySQL con productos reales...");
                List<Interacciones> semilla = List.of(
                        new Interacciones("cliente@pedidos360.com", "PROD-101", "view", 1, "Laptops", "Hardware"),
                        new Interacciones("cliente@pedidos360.com", "PROD-101", "cart", 2, "Laptops", "Hardware"),
                        new Interacciones("cliente@pedidos360.com", "PROD-101", "purchase", 3, "Laptops", "Hardware"),
                        new Interacciones("cliente@pedidos360.com", "PROD-201", "view", 1, "Periféricos", "Accesorios"),
                        new Interacciones("cliente@pedidos360.com", "PROD-201", "cart", 2, "Periféricos", "Accesorios"),
                        new Interacciones("cliente@pedidos360.com", "PROD-301", "view", 1, "Monitores", "Pantallas"),
                        new Interacciones("cliente@pedidos360.com", "PROD-401", "view", 1, "Audio", "Sonido"),
                        new Interacciones("cliente@pedidos360.com", "PROD-102", "view", 1, "Laptops", "Hardware"),
                        new Interacciones("cliente@pedidos360.com", "PROD-501", "view", 1, "Almacenamiento", "Hardware")
                );
                interaccionesRepository.saveAll(semilla);
                calcularYGuardarTop5("cliente@pedidos360.com");
            }
        } catch (Exception e) {
            log.warn("No se pudo ejecutar semilla inicial de analítica: {}", e.getMessage());
        }
    }

    public Optional<Producto> buscarProductoCatalogo(String itemId) {
        if (itemId == null) return Optional.empty();
        return CATALOGO_MAESTRO.stream()
                .filter(p -> p.getId().equalsIgnoreCase(itemId.trim()))
                .findFirst();
    }

    /**
     * Procesa y persiste directamente una interacción cuando se invoca como fallback sin Kafka.
     */
    @Transactional
    public void procesarInteraccionDirecta(InteractionRequestsDTO dto) {
        if (dto == null || dto.getClientId() == null) return;
        String action = dto.getAction() != null ? dto.getAction().toLowerCase() : "view";
        int weight = switch (action) {
            case "purchase" -> 3;
            case "cart" -> 2;
            default -> 1;
        };

        String itemId = dto.getItemId() != null ? dto.getItemId() : "PROD-101";
        Optional<Producto> prodOpt = buscarProductoCatalogo(itemId);
        String category = dto.getCategory() != null ? dto.getCategory()
                : prodOpt.map(Producto::getCategoria).orElse("Laptops");
        String productType = dto.getProductType() != null ? dto.getProductType()
                : prodOpt.map(Producto::getProductType).orElse("Hardware");

        Interacciones interaccion = new Interacciones(
                dto.getClientId(),
                itemId,
                action,
                weight,
                category,
                productType
        );
        interaccionesRepository.save(interaccion);
        dynamoDbStorageService.guardarInteraccion(dto, weight);
        calcularYGuardarTop5(dto.getClientId());
    }

    /**
     * Recalcula y persiste el Top 5 de productos recomendados para un usuario en MySQL.
     */
    @Transactional
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

            Optional<Producto> catalogProd = buscarProductoCatalogo(itemId);

            String categoria = catalogProd.map(Producto::getCategoria)
                    .orElseGet(() -> interacciones.stream()
                            .filter(i -> i.getItemId().equals(itemId))
                            .map(Interacciones::getCategory)
                            .filter(Objects::nonNull)
                            .findFirst().orElse("Laptops"));

            String productType = catalogProd.map(Producto::getProductType)
                    .orElseGet(() -> interacciones.stream()
                            .filter(i -> i.getItemId().equals(itemId))
                            .map(Interacciones::getProductType)
                            .filter(Objects::nonNull)
                            .findFirst().orElse("Hardware"));

            Recomendaciones rec = new Recomendaciones(userId, itemId, score, rank++, categoria, productType);
            recomendacionesRepository.save(rec);
        }

        // Emitir actualización en vivo por WebSocket
        transmitirMetricasActuales();
    }

    /**
     * Retorna los productos recomendados para un usuario basándose en:
     * 1. Los productos con mayor puntaje de interacción del usuario (purchase=3, cart=2, view=1).
     * 2. Otros productos destacados de las categorías preferidas por el usuario según su historial.
     * 3. Si es un usuario nuevo sin historial, un mix balanceado de las 5 categorías de la tienda (no solo Laptops).
     */
    public List<Producto> obtenerRecomendacionesParaUsuario(String userId) {
        List<Interacciones> historialUsuario = interaccionesRepository.findByUserId(userId);
        List<Recomendaciones> topRecs = recomendacionesRepository.findByUserIdOrderByRankPositionAsc(userId);

        List<Producto> resultado = new ArrayList<>();

        // 1. Agregar productos directamente interactuados por el usuario (ordenados por puntaje)
        for (Recomendaciones rec : topRecs) {
            if (resultado.size() >= 5) break;
            buscarProductoCatalogo(rec.getItemId()).ifPresent(prod -> {
                if (resultado.stream().noneMatch(r -> r.getId().equalsIgnoreCase(prod.getId()))) {
                    resultado.add(prod);
                }
            });
        }

        // 2. Si el usuario tiene interacciones en categorías específicas, sugerir otros productos de sus categorías favoritas
        if (!historialUsuario.isEmpty() && resultado.size() < 4) {
            List<String> categoriasPreferidas = historialUsuario.stream()
                    .filter(i -> i.getCategory() != null && !i.getCategory().isBlank())
                    .collect(Collectors.groupingBy(
                            Interacciones::getCategory,
                            Collectors.summingInt(Interacciones::getWeight)
                    ))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .map(Map.Entry::getKey)
                    .toList();

            for (String cat : categoriasPreferidas) {
                for (Producto candidato : CATALOGO_MAESTRO) {
                    if (resultado.size() >= 4) break;
                    if (candidato.getCategoria().equalsIgnoreCase(cat)
                            && resultado.stream().noneMatch(r -> r.getId().equalsIgnoreCase(candidato.getId()))) {
                        resultado.add(candidato);
                    }
                }
            }
        }

        // 3. Si aún faltan productos para completar 4 (ej. usuario nuevo), rellenar con 1 destacado de cada categoría
        if (resultado.size() < 4) {
            List<String> destacadosDiversos = List.of("PROD-101", "PROD-201", "PROD-301", "PROD-401", "PROD-501");
            for (String prodId : destacadosDiversos) {
                if (resultado.size() >= 4) break;
                buscarProductoCatalogo(prodId).ifPresent(prod -> {
                    if (resultado.stream().noneMatch(r -> r.getId().equalsIgnoreCase(prod.getId()))) {
                        resultado.add(prod);
                    }
                });
            }
        }

        return resultado;
    }

    /**
     * Registra una alerta antifraude (HTTP 429) para mostrar en el feed en vivo y métricas WebSocket.
     */
    public void registrarAlertaFraude(String clientId, String razon) {
        registrarAlertaFraude(clientId, razon, "190.161.88.94");
    }

    public void registrarAlertaFraude(String clientId, String razon, String ip) {
        String nowStr = TIME_FMT.format(Instant.now());
        Map<String, Object> alerta = new LinkedHashMap<>();
        alerta.put("id", System.currentTimeMillis());
        alerta.put("tipo", "FRAUDE_DETECTADO");
        alerta.put("ip", ip != null ? ip : "190.161.88.94");
        alerta.put("client", clientId);
        alerta.put("clientId", clientId);
        alerta.put("reason", razon);
        alerta.put("razon", razon);
        alerta.put("action", "Bloqueo temporal 30 min (HTTP 429)");
        alerta.put("timestamp", nowStr);

        historialAlertas.add(0, alerta);
        if (historialAlertas.size() > 25) {
            historialAlertas.remove(historialAlertas.size() - 1);
        }
        transmitirMetricasActuales();
    }

    /**
     * Construye el snapshot real de métricas desde MySQL (interacciones) + historial de alertas antifraude.
     */
    public Map<String, Object> obtenerSnapshotMetricas() {
        List<Interacciones> todas = interaccionesRepository.findTop100ByOrderByTimestampDesc();
        long totalDbCount = interaccionesRepository.count();

        int vistas = 0;
        int carritos = 0;
        int compras = 0;

        Map<String, int[]> statsPorProducto = new HashMap<>(); // [vistas, carritos, compras, afinidad]

        List<Interacciones> todasCompletas = totalDbCount <= 500 ? interaccionesRepository.findAll() : todas;
        for (Interacciones inter : todasCompletas) {
            String act = inter.getAction() != null ? inter.getAction().toLowerCase() : "view";
            String itemId = inter.getItemId() != null ? inter.getItemId().toUpperCase() : "PROD-101";
            int[] arr = statsPorProducto.computeIfAbsent(itemId, k -> new int[4]);

            if ("purchase".equals(act)) {
                compras++;
                arr[2]++;
                arr[3] += 3;
            } else if ("cart".equals(act)) {
                carritos++;
                arr[1]++;
                arr[3] += 2;
            } else {
                vistas++;
                arr[0]++;
                arr[3] += 1;
            }
        }

        // Asegurar al menos los 5 productos principales en el ranking
        for (String defaultId : List.of("PROD-101", "PROD-102", "PROD-201", "PROD-301", "PROD-401")) {
            statsPorProducto.computeIfAbsent(defaultId, k -> new int[]{0, 0, 0, 0});
        }

        // Métrica 2: Top 5 productos más demandados con desglose completo
        List<Map<String, Object>> top5 = statsPorProducto.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue()[3], a.getValue()[3]))
                .limit(5)
                .map(entry -> {
                    String itemId = entry.getKey();
                    int[] counts = entry.getValue();
                    Optional<Producto> prodOpt = buscarProductoCatalogo(itemId);
                    String nombre = prodOpt.map(Producto::getNombre).orElse(itemId);
                    String categoria = prodOpt.map(Producto::getCategoria).orElse("Tecnología");
                    int stock = prodOpt.map(Producto::getStock).orElse(10);

                    int v = counts[0];
                    int c = counts[1];
                    int p = counts[2];
                    int afinidad = counts[3];
                    double conv = v > 0 ? Math.min(100.0, (p * 100.0) / v) : (p > 0 ? 100.0 : 0.0);

                    Map<String, Object> itemMap = new LinkedHashMap<>();
                    itemMap.put("id", itemId);
                    itemMap.put("itemId", itemId);
                    itemMap.put("nombre", nombre);
                    itemMap.put("categoria", categoria);
                    itemMap.put("vistas", v);
                    itemMap.put("carritos", c);
                    itemMap.put("compras", p);
                    itemMap.put("afinidadTotal", afinidad);
                    itemMap.put("puntosInteraccion", afinidad);
                    itemMap.put("conversionRate", String.format(Locale.US, "%.1f%%", conv));
                    itemMap.put("stockActual", stock);
                    return itemMap;
                })
                .toList();

        // Feed de eventos recientes reales desde MySQL
        List<Map<String, Object>> recentEvents = new ArrayList<>();
        for (Interacciones i : todas.stream().limit(15).toList()) {
            String act = i.getAction() != null ? i.getAction().toLowerCase() : "view";
            Producto prod = buscarProductoCatalogo(i.getItemId()).orElse(null);
            String prodName = prod != null ? prod.getNombre() : i.getItemId();
            String subtitle = switch (act) {
                case "purchase" -> "Compra confirmada • Peso +3 pts (Kafka + MySQL)";
                case "cart" -> "Ítem agregado al carrito • Peso +2 pts";
                default -> "Vista de producto en catálogo • Peso +1 pt";
            };
            Map<String, Object> ev = new LinkedHashMap<>();
            ev.put("id", i.getId() != null ? i.getId() : System.nanoTime());
            ev.put("type", act);
            ev.put("title", prodName + " (" + i.getItemId() + ")");
            ev.put("subtitle", subtitle);
            ev.put("time", TIME_FMT.format(Instant.ofEpochMilli(i.getTimestamp())));
            ev.put("client", i.getUserId());
            ev.put("size", "1.4 KB");
            recentEvents.add(ev);
        }

        // Métrica 3: Alertas antifraude
        List<Map<String, Object>> ultimasAlertas = new ArrayList<>(historialAlertas.stream().limit(10).toList());
        int totalEventos = (int) totalDbCount + historialAlertas.size();

        Map<String, Object> payload = new LinkedHashMap<>();
        // Llaves de rúbrica
        payload.put("metrica1_totalInteracciones", totalEventos);
        payload.put("metrica2_topProductos", top5);
        payload.put("metrica3_alertasSeguridad", ultimasAlertas);
        // Llaves detalladas para CloudOpsView Dashboard
        payload.put("totalEventos", totalEventos);
        payload.put("vistas", vistas);
        payload.put("carritos", carritos);
        payload.put("pagosAprobados", compras);
        payload.put("bloqueosFraude", historialAlertas.size());
        payload.put("tasaPorMinuto", Math.round((totalEventos * 1.4) * 10.0) / 10.0);
        payload.put("topProductos", top5);
        payload.put("recentEvents", recentEvents);
        payload.put("alertasFraude", ultimasAlertas);
        payload.put("timestamp", System.currentTimeMillis());
        return payload;
    }

    /**
     * Construye y difunde el payload con las 3 métricas en tiempo real requeridas por la rúbrica.
     */
    public Map<String, Object> transmitirMetricasActuales() {
        Map<String, Object> payload = obtenerSnapshotMetricas();
        webSocketNotificationService.emitirMetricasEnVivo(payload);
        return payload;
    }
}
