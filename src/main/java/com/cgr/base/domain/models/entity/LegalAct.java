package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.Set;

@Data
@Entity
@Table(name = "legal_acts")
public class LegalAct {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "legal_act_id")
    private Long id;

    @Column(name = "legal_act_no", nullable = false, unique = true)
    private String legalActNo;

    // @Column(name = "contract_year_acceptance", nullable = false)
    // private Integer contractYearAcceptance;

    @Column(name = "initial_legal_act_value", nullable = false)
    private float initialLegalActValue;

    @Column(name = "legal_act_end_date")
    private Date legalActEndDate;

    @ManyToMany
    @JoinTable(name = "legal_act_source_financing", joinColumns = @JoinColumn(name = "legal_act_id"), inverseJoinColumns = @JoinColumn(name = "source_financing_id"))
    private Set<EntitySourceFinance> sourcesFinance;

    @ManyToMany
    @JoinTable(name = "indicators_legal_acts", joinColumns = @JoinColumn(name = "legal_act_id"), inverseJoinColumns = @JoinColumn(name = "indicator_id"))
    private Set<EntityIndicator> indicators;

}
