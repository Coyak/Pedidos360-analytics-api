package com.kafka.recomendaciones.DTO;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InteractionRequestsDTO {
    private String clientId;
    private String itemId;
    private String action; // view, cart, purchase
    private String category;
    private String productType;
    private Long timestamp;
}
