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
@Table(name = "Expenses")
public class Expense {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int expenseId;

    @ManyToOne
    @JoinColumn(name = "programContractId")
    private Program program;

    @Column(nullable = false, length = 30)
    private String category;

    @Column(nullable = false)
    private double amount;

    @Column(nullable = false)
    @Temporal(TemporalType.DATE)
    private Date expenseDate;

    // Getters and Setters
}
