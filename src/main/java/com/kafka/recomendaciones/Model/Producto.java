package com.kafka.recomendaciones.Model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Producto {
    private String id;
    private String nombre;
    private String categoria;
    private String productType;
    private String marca;
    private int precio;
    private double rating;
    private String imagen;
    private int stock;
}
