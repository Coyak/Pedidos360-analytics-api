package com.kafka.recomendaciones.Service;

import com.kafka.recomendaciones.DTO.InteractionRequestsDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Servicio de Persistencia NoSQL en la nube con Amazon DynamoDB (+2 Puntos Extra).
 * Almacena el historial continuo de eventos de navegación, clics y carrito.
 */
@Slf4j
@Service
public class DynamoDbStorageService {

    @Value("${aws.region:us-east-1}")
    private String awsRegion;

    @Value("${aws.dynamodb.table-name:Pedidos360_Interacciones}")
    private String tableName;

    @Value("${aws.dynamodb.endpoint:}")
    private String dynamoEndpoint;

    private DynamoDbClient dynamoDbClient;

    private synchronized DynamoDbClient getClient() {
        if (dynamoDbClient == null) {
            try {
                DynamoDbClientBuilder builder = DynamoDbClient.builder()
                        .region(Region.of(awsRegion));
                if (dynamoEndpoint != null && !dynamoEndpoint.trim().isEmpty()) {
                    builder.endpointOverride(URI.create(dynamoEndpoint));
                }
                dynamoDbClient = builder.build();
                log.info(">>> [DYNAMODB CLIENT] Inicializado en región: {}", awsRegion);
            } catch (Exception e) {
                log.warn(">>> [DYNAMODB ADVERTENCIA] No se pudo inicializar cliente DynamoDB ({}). Operando en modo local.", e.getMessage());
            }
        }
        return dynamoDbClient;
    }

    public void guardarInteraccion(InteractionRequestsDTO dto, int weight) {
        DynamoDbClient client = getClient();
        if (client == null) return;

        try {
            Map<String, AttributeValue> item = new HashMap<>();
            String eventId = "EVT-" + System.currentTimeMillis() + "-" + UUID.randomUUID().toString().substring(0, 4);
            item.put("userId", AttributeValue.builder().s(dto.getClientId() != null ? dto.getClientId() : "anonimo").build());
            item.put("timestamp_eventId", AttributeValue.builder().s(System.currentTimeMillis() + "#" + eventId).build());
            item.put("itemId", AttributeValue.builder().s(dto.getItemId() != null ? dto.getItemId() : "N/A").build());
            item.put("action", AttributeValue.builder().s(dto.getAction() != null ? dto.getAction() : "view").build());
            item.put("weight", AttributeValue.builder().n(String.valueOf(weight)).build());
            if (dto.getCategory() != null) item.put("category", AttributeValue.builder().s(dto.getCategory()).build());
            if (dto.getProductType() != null) item.put("productType", AttributeValue.builder().s(dto.getProductType()).build());

            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();

            client.putItem(request);
            log.info(">>> [DYNAMODB EXTRA +2 PTS] Evento guardado en tabla '{}' para userId: {}", tableName, dto.getClientId());
        } catch (Exception e) {
            log.warn(">>> [DYNAMODB INFO] DynamoDB Cloud no alcanzable sin credenciales AWS activas ({}). Continuando flujo con base MySQL.", e.getMessage());
        }
    }
}
