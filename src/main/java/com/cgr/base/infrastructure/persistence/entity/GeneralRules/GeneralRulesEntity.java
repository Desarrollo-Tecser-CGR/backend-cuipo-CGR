package com.cgr.base.infrastructure.persistence.entity.GeneralRules;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "general_rules")
@Data
@NoArgsConstructor
public class GeneralRulesEntity {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(nullable = true)
    private String period;

    @Column(nullable = true)
    private String entityAmbit;

    @Column(nullable = true)
    private String entityName;

    @Column(nullable = true)
    private String accountName;

    @Column(nullable = true)
    private String generalRule1;

    @Column(nullable = true)
    private String generalRule2;

    @Column(nullable = true)
    private String generalRule4;
}
