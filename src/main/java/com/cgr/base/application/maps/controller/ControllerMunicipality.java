package com.cgr.base.application.maps.controller;

import com.cgr.base.application.maps.entity.dtoMaps.municipality.MunicipalityDto;
import com.cgr.base.application.maps.servicesMaps.ServicesMunicipalitie;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
@RequestMapping (path = "/api/v1/municipality")
@RestController
public class ControllerMunicipality {

    @Autowired
    private ServicesMunicipalitie servicesMunicipalitie;

    @GetMapping(path = "/{id}")
    public ResponseEntity<Map<String, Object>> municipality(@PathVariable Long id) {
        return servicesMunicipalitie.searchMunicipality(id);
    }

    @GetMapping(path = "/mp/{id}")
    public ResponseEntity<Map<String, Object>> getMunicipalityById(@PathVariable String id) {
        return servicesMunicipalitie.searchMunicipalityDp(id);
    }
}
