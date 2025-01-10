package com.cgr.base.infrastructure.persistence.entity.contracts;

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
@Table(name = "Contracts")
public class Contract {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int contractId;

    @ManyToOne
    @JoinColumn(name = "programId")
    private Program program;

    @Temporal(TemporalType.DATE)
    private Date startDate;

    @Temporal(TemporalType.DATE)
    private Date endDate;

    @Column(nullable = false)
    private double totalAmount;

    @Column(nullable = false, length = 30)
    private String status;

    @OneToMany(mappedBy = "contract")
    private List<Support> supports;

    @OneToMany(mappedBy = "contract")
    private List<ContractExecutionTracking> trackings;

    @OneToMany(mappedBy = "contract")
    private List<ContractIndicator> indicators;

    @OneToMany(mappedBy = "contract")
    private List<ContractModificationHistory> modificationHistories;

    // Getters and Setters
}