package com.cgr.base.infrastructure.persistence.entity;

import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Entity
@Table(name = "Beneficiaries")
public class Beneficiary {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int beneficiaryId;

    @Column(nullable = false, length = 30)
    private String beneficiaryType;

    @Column(nullable = false, length = 250)
    private String identificationName;

    @Column(nullable = false, length = 250)
    private String locationMunicipality;

    @Column(nullable = false)
    private int associatedProgram;

    @OneToMany(mappedBy = "beneficiary")
    private List<BeneficiaryProgram> beneficiaryPrograms;

    // Getters and Setters
}
