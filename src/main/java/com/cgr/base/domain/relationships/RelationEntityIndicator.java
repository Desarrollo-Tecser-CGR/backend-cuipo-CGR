package com.cgr.base.domain.relationships;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Table(name = " [entities_indicators")
public class RelationEntityIndicator {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long entity_indicator_id;
    private Integer entity_id;
    private Integer indicator_id;

}
