package com.cgr.base.entity.certifications;

import java.io.Serializable;
import java.time.LocalDateTime;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@Data
@DynamicInsert
@DynamicUpdate
@Table(name = "CONTROL_CERTIFICACION")
@IdClass(certificationEntity.certificationId.class)
public class certificationEntity {

    @Id
    @Column(name = "FECHA")
    private Integer fecha;

    @Id
    @Column(name = "CODIGO_ENTIDAD")
    private String codigoEntidad;

    @Column(name = "NOMBRE_ENTIDAD")
    private String nombreEntidad;

    @Column(name = "PORCENTAJE_CALIDAD")
    private String porcentajeCalidad;

    @Column(name = "ESTADO_CALIDAD")
    private String estadoCalidad;

    @Column(name = "FECHA_ACT_CALIDAD")
    private LocalDateTime fechaActCalidad;

    @Column(name = "USER_ACT_CALIDAD")
    private String userActCalidad;

    @Column(name = "OBSERVACION_CALIDAD")
    private String observacionCalidad;

    @Column(name = "PORCENTAJE_L617")
    private String porcentajeL617;

    @Column(name = "ESTADO_L617")
    private String estadoL617;

    @Column(name = "FECHA_ACT_L617")
    private LocalDateTime fechaActL617;

    @Column(name = "USER_ACT_L617")
    private String userActL617;

    @Column(name = "OBSERVACION_L617")
    private String observacionL617;

    @Data
    @NoArgsConstructor
    public static class certificationId implements Serializable {
        private Integer fecha;
        private String codigoEntidad;
    }

}
