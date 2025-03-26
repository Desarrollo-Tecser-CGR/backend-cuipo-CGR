package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.Set;

@Data
@Entity
@Table(name = "contracts")
public class Contract {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "contract_id")
    private Long id;

    @Column(name = "contract_no", nullable = false, unique = true)
    private String contractNo;

    @Column(name = "contract_year_acceptance", nullable = false)
    private Integer contractYearAcceptance;

    @Column(name = "contract_start_date", nullable = false)
    private Date contractStartDate;

    @Column(name = "contract_end_date")
    private Date contractEndDate;

    @ManyToMany
    @JoinTable(name = "contracts_sources_financing", joinColumns = @JoinColumn(name = "contract_id"), inverseJoinColumns = @JoinColumn(name = "source_financing_id"))
    private Set<EntitySourceFinance> sourcesFinance;

    @Column(name = "contractor_id", nullable = false)
    private Long contractorId;

    @ManyToMany
    @JoinTable(name = "indicators_contracts", joinColumns = @JoinColumn(name = "contract_id"), inverseJoinColumns = @JoinColumn(name = "indicator_id"))
    private Set<EntityIndicator> indicators;

}
