package com.cgr.base.domain.models.entity;

import java.util.Date;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.fasterxml.jackson.annotation.JsonFormat;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import org.springframework.messaging.handler.annotation.SendTo;

@AllArgsConstructor
@NoArgsConstructor
@Entity
@Getter
@Setter
@Table(name = "notification")
public class EntityNotification {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column
    private String numbercontract;

    @Column
    private String subject;

    @Column
    private String notification;

    @NotNull
    @Column(name = "date", updatable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "America/Bogota")
    private Date date;

    @ManyToOne
    @JoinColumn(name = "entity_id", nullable = false)
    private EntityProvitionalPlan entity;

    @ManyToOne
    @JoinColumn(name = "user_id", nullable = false) // Agregamos la relación con el usuario
    private UserEntity user;

}
