package com.test.testactivedirectory.infrastructure.persistence.entity.ventas;



import java.util.Date;


import jakarta.validation.constraints.NotNull;  
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name="Ventas")
public class VentasEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_ventas")
    private Integer idVentas ;
    @Column(name = "nombre_venta", nullable = false, length = 50)
    @NotNull(message = "El campo nombre no puede ser nulo")
    private String nombre ;
    private String cliente ;
    private String fecha ;

    
}
