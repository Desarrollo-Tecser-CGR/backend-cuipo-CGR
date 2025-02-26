package com.cgr.base.application.maps.entity.dtoMaps.deparment;

import com.cgr.base.application.maps.entity.dtoMaps.municipality.MunicipalityDto;
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

