package com.test.testactivedirectory.infrastructure.persistence.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "ContractModificationHistory")
public class ContractModificationHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int modificationId;

    @ManyToOne
    @JoinColumn(name = "contractId")
    private Contract contract;

    @Column(nullable = false, length = 65533)
    private String modificationType;

    @Column(nullable = false, length = 65533)
    private String modificationDescription;

    @Column(nullable = false, length = 250)
    private String approvedBy;

    // Getters and Setters
}

