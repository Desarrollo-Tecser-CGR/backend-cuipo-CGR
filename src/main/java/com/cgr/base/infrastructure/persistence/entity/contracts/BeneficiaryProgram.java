package com.cgr.base.infrastructure.persistence.entity.contracts;

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
@Table(name = "Beneficiaries_Programs")
public class BeneficiaryProgram {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @ManyToOne
    @JoinColumn(name = "beneficiaryId")
    private Beneficiary beneficiary;

    @ManyToOne
    @JoinColumn(name = "programId")
    private Program program;

    // Getters and Setters
}