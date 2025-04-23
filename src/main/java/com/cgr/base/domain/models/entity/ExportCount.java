package com.cgr.base.domain.models.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Date;

@Data
@Entity
@Table(name = "export_count")
public class ExportCount {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(columnDefinition = "integer")
    private Long id;

    @Column(name = "export_date")
    private Date exportDate;

    @Column(name = "export_count")
    private Integer exportCount;

    @Column(name = "export_type")
    private String exportType;
}