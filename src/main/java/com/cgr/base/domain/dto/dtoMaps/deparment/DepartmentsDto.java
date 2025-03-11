package com.cgr.base.domain.dto.dtoMaps.deparment;

import com.cgr.base.domain.dto.dtoMaps.municipality.MunicipalityDto;
import lombok.Data;

import java.util.List;

@Data
public class DepartmentsDto {

    private Long department_id;
    private String dpto_cnmbr;
    private String dpto_ccdgo;
    private String image;
    private List<MunicipalityDto> municipalities;
}