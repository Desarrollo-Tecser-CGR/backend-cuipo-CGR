package com.cgr.base.domain.models.entity;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Entity
@Data
@Table(name = "entities")
public class EntityProvitionalPlan {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer entity_id;
    @Column(name = "entity_nit")
    private String entity_nit;
    @Column(name = "entity_name")
    private String entity_name;

    @ManyToMany
    @JsonIgnore
    @JoinTable(name = "entities_indicators", joinColumns = @JoinColumn(name = "entity_id"), inverseJoinColumns = @JoinColumn(name = "indicator_id"))
    private Set<EntityIndicator> indicators;

    @JsonIgnore
    @OneToMany(mappedBy = "entity", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<EntityNotification> notifications;
}
