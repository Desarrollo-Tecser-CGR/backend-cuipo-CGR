package com.test.testactivedirectory.infrastructure.persistence.entity;

import java.util.Date;
import java.util.List;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.Temporal;
import jakarta.persistence.TemporalType;
import lombok.Data;

@Entity
@Data
@Table(name = "Supports")
public class Support {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int supportId;

    @ManyToOne
    @JoinColumn(name = "contractId")
    private Contract contract;

    @Column(nullable = false)
    private int supportType;

    @Column(nullable = false, length = 65533)
    private String description;

    @Temporal(TemporalType.DATE)
    private Date issueDate;

    @Column(nullable = false, length = 65533)
    private String digitalFile;

    @Column(nullable = false, length = 250)
    private String issueResponsible;

    @Column(nullable = false, length = 30)
    private String supportStatus;

    @OneToMany(mappedBy = "support")
    private List<SupportHistory> histories;

    // Getters and Setters
}
