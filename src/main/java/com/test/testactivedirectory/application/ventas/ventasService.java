package com.test.testactivedirectory.application.ventas;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import com.test.testactivedirectory.infrastructure.persistence.entity.ventas.VentasEntity;
import com.test.testactivedirectory.infrastructure.persistence.repository.ventas.VentasRepository;

import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class ventasService {
    private VentasRepository ventasRepository;
    public Optional<?> getalList (){
        return ventasRepository.findById(6);
    }
}
