package com.test.testactivedirectory.infrastructure.persistence.entity;

import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.Data;

@Entity
@Data
@Table(name = "Programs")
public class Program {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int programId;

    @Column(nullable = false, length = 250)
    private String programName;

    @Column(nullable = false, length = 65533)
    private String generalObjective;

    @Column(nullable = false, length = 65533)
    private String specificObjective;

    @Column(nullable = false)
    private int implementationYear;

    @Column(nullable = false)
    private int responsibleEntity;

    @OneToMany(mappedBy = "program")
    private List<BeneficiaryProgram> beneficiaryPrograms;

    @OneToMany(mappedBy = "program")
    private List<Expense> expenses;

    @OneToMany(mappedBy = "program")
    private List<Contract> contracts;

    // Getters and Setters
}
