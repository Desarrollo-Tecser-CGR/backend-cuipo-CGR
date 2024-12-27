package com.test.testactivedirectory.infrastructure.persistence.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.util.Date;
import java.util.List;

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
