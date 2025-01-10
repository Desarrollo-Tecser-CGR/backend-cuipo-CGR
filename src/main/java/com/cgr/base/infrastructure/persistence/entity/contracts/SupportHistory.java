package com.cgr.base.infrastructure.persistence.entity.contracts;

import java.util.Date;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Entity
@Data
@Table(name = "SupportHistory")
public class SupportHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int historyId;

    @ManyToOne
    @JoinColumn(name = "supportId")
    private Support support;

    @Column(nullable = false, length = 65533)
    private String performedAction;

    @Temporal(TemporalType.DATE)
    private Date actionDate;

    @Column(nullable = false, length = 250)
    private String responsibleUser;

    @Column(nullable = false, length = 65533)
    private String comments;

    // Getters and Setters
}
