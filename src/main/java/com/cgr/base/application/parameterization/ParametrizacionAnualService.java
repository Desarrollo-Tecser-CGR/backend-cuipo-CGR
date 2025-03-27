package com.cgr.base.application.parameterization;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;
import com.cgr.base.infrastructure.persistence.repository.parametrization.ParametrizacionAnualRepository;

import jakarta.transaction.Transactional;

import java.math.BigDecimal;
import java.time.Year;

@Service
public class ParametrizacionAnualService {

    @Autowired
    private ParametrizacionAnualRepository parametrizacionAnualRepository;

    @Transactional
    public ParametrizacionAnual save(ParametrizacionAnual parametrizacionAnual) {
        calcularLimIcld(parametrizacionAnual);
        validarFecha(parametrizacionAnual.getFecha());
        return parametrizacionAnualRepository.save(parametrizacionAnual);
    }

    @Transactional
    public ParametrizacionAnual update(ParametrizacionAnual parametrizacionAnual) {
        calcularLimIcld(parametrizacionAnual);
        System.out.println("Limite ICLD calculado PRINCIPAL: " + parametrizacionAnual.getLimIcld());
        if (validarFecha(parametrizacionAnual.getFecha())) {
            return parametrizacionAnualRepository.save(parametrizacionAnual);
        } else {
            return null;
        }

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

    private void calcularLimIcld(ParametrizacionAnual parametrizacionAnual) {
        int anioActual = parametrizacionAnual.getFecha();
        Optional<ParametrizacionAnual> anioAnterior = parametrizacionAnualRepository.findByFecha(anioActual - 1);

        if (anioAnterior.isPresent()) {
            double limIclDAnterior = anioAnterior.get().getLimIcld().doubleValue();
            double ipcAnioAnterior = anioAnterior.get().getIpc().doubleValue();
            double nuevoLimIclD = limIclDAnterior + (limIclDAnterior * ipcAnioAnterior);
            parametrizacionAnual.setLimIcld(BigDecimal.valueOf(nuevoLimIclD));
        } else {
            parametrizacionAnual.setLimIcld(BigDecimal.valueOf(0.0));
        }

        System.out.println("Limite ICLD calculado: " + parametrizacionAnual.getLimIcld());
    }

    private boolean validarFecha(int fecha) {
        int anioActual = Year.now().getValue();
        if (fecha != anioActual && fecha != anioActual - 1) {
            return false;
        } else {
            return true;
        }
    }
}
