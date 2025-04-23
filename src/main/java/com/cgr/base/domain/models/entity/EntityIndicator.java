package com.cgr.base.domain.models.entity;

import java.util.Set;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "indicators")
public class EntityIndicator {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer indicator_id;

    private Integer indicator_action_id;
    @Column(columnDefinition = "varchar(max)")
    private String indicator_name;

    @Column(columnDefinition = "float")
    private Integer total_indicator_budget;

    @ManyToMany(mappedBy = "indicators")
    private Set<LegalAct> contracts;

    @ManyToMany
    @JoinTable(name = "entities_indicators", joinColumns = @JoinColumn(name = "indicator_id"), inverseJoinColumns = @JoinColumn(name = "entity_id"))
    private Set<EntityProvitionalPlan> entityProvisionalPlans;

}