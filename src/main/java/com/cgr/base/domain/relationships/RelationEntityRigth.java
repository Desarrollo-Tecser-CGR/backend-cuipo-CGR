package com.cgr.base.domain.relationships;

import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;

@Data
@Table (name = " entities_rights")
public class RelationEntityRigth {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long entity_right_id ;
        private Integer entity_id;
        private Integer right_id;
}
