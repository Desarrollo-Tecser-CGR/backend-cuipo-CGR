package com.cgr.base.application.services.module.serviceModule.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ContracViewDto {

    private Long entityId;
    private String entityName;
    private Double totalValuePaid;
    private String contractorName;
}
