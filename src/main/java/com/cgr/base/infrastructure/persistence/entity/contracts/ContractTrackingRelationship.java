package com.cgr.base.infrastructure.persistence.entity.contracts;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "ContractTrackingRelationship")
public class ContractTrackingRelationship {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int relationshipId;

    @Column(nullable = false, length = 65533)
    private String startCertificate;

    @Column(nullable = false, length = 65533)
    private String progressReports;

    @Column(nullable = false, length = 65533)
    private String deliveryCertificate;

    @Column(nullable = false, length = 65533)
    private String paymentReceipts;

    // Getters and Setters
}