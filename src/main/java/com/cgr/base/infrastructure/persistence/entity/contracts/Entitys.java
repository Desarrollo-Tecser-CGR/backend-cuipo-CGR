package com.cgr.base.infrastructure.persistence.entity.contracts;

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
@Table(name = "Entities")
public class Entitys {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int entityId;

    @Column(nullable = false, length = 250)
    private String name;

    @Column(nullable = false, length = 30)
    private String type;

    @Column(nullable = false, length = 250)
    private String sector;

    @Column(nullable = false, length = 250)
    private String contactInformation;

    @OneToMany(mappedBy = "responsibleEntity")
    private List<Program> programs;

    // Getters and Setters
}
