package com.cgr.base.infrastructure.security.endpoints;

import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.DynamicUpdate;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor
@Data
@DynamicInsert
@DynamicUpdate
@Table(name = "endpoints")
public class endpointEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @Column(name = "url", nullable = false, length = 255)
    private String url;

    @Column(name = "type", nullable = false, length = 20)
    private String type;

}
