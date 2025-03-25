package com.cgr.base.presentation.parametrization;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rulesEngine.parameterization.ParametrizacionAnualService;
import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;

@RestController
@RequestMapping("/api/v1/parametrization/annual")
public class ParametrizacionAnualController {

    @Autowired
    private ParametrizacionAnualService parametrizacionAnualService;

    @GetMapping
    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualService.getAll();
    }

    @GetMapping("/{fecha}")
    public ResponseEntity<ParametrizacionAnual> getByFecha(@PathVariable int fecha) {
        return parametrizacionAnualService.getByFecha(fecha)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<ParametrizacionAnual> create(@RequestBody ParametrizacionAnual parametrizacionAnual) {
        return ResponseEntity.ok(parametrizacionAnualService.save(parametrizacionAnual));
    }

    @PutMapping
    public ResponseEntity<ParametrizacionAnual> update(@RequestBody ParametrizacionAnual parametrizacionAnual) {
        return ResponseEntity.ok(parametrizacionAnualService.update(parametrizacionAnual));
    }

    @DeleteMapping("/{fecha}")
    public void deleteByFecha(@PathVariable int fecha) {
        parametrizacionAnualService.deleteByFecha(fecha);
    }

}
