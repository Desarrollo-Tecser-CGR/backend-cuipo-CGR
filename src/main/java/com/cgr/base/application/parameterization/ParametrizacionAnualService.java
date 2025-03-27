package com.cgr.base.application.parameterization;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;
import com.cgr.base.infrastructure.persistence.repository.parametrization.ParametrizacionAnualRepository;

import jakarta.transaction.Transactional;

@Service
public class ParametrizacionAnualService {

    @Autowired
    private ParametrizacionAnualRepository parametrizacionAnualRepository;

    @Transactional
    public ParametrizacionAnual save(ParametrizacionAnual parametrizacionAnual) {
        return parametrizacionAnualRepository.save(parametrizacionAnual);
    }

    @Transactional
    public ParametrizacionAnual update(ParametrizacionAnual parametrizacionAnual) {
        return parametrizacionAnualRepository.update(parametrizacionAnual);
    }

    public Optional<ParametrizacionAnual> getByFecha(int fecha) {
        return parametrizacionAnualRepository.findByFecha(fecha);
    }

    @Transactional
    public void deleteByFecha(int fecha) {
        parametrizacionAnualRepository.deleteByFecha(fecha);
    }

    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualRepository.findAll();
    }
}
