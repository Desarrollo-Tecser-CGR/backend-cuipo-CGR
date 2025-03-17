package com.cgr.base.application.rulesEngine.parameterization;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;
import com.cgr.base.infrastructure.persistence.repository.parametrization.ParametrizacionAnualRepository;

import java.util.List;
import java.util.Optional;

@Service
public class ParametrizacionAnualService {

    @Autowired
    private ParametrizacionAnualRepository parametrizacionAnualRepository;

    public ParametrizacionAnual saveOrUpdate(ParametrizacionAnual parametrizacionAnual) {
        return parametrizacionAnualRepository.save(parametrizacionAnual);
    }

    public Optional<ParametrizacionAnual> getByFecha(int fecha) {
        return parametrizacionAnualRepository.findByFecha(fecha);
    }

    public void deleteByFecha(int fecha) {
        parametrizacionAnualRepository.deleteByFecha(fecha);
    }

    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualRepository.findAll();
    }
}
