package com.cgr.base.presentation.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import com.cgr.base.application.rulesEngine.parameterization.ParametrizacionAnualService;
import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;

import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/api/v1/parametrizacion")
public class ParametrizacionAnualController {

    @Autowired
    private ParametrizacionAnualService parametrizacionAnualService;

    @GetMapping
    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualService.getAll();
    }

    @GetMapping("/{fecha}")
    public Optional<ParametrizacionAnual> getByFecha(@PathVariable int fecha) {
        return parametrizacionAnualService.getByFecha(fecha);
    }

    @PostMapping
    public ParametrizacionAnual createOrUpdate(@RequestBody ParametrizacionAnual parametrizacionAnual) {
        return parametrizacionAnualService.saveOrUpdate(parametrizacionAnual);
    }

    @DeleteMapping("/{fecha}")
    public void deleteByFecha(@PathVariable int fecha) {
        parametrizacionAnualService.deleteByFecha(fecha);
    }
}
