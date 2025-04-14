package com.cgr.base.service.certifications;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.repository.certifications.certificationsRepo;

import jakarta.persistence.EntityNotFoundException;

@Service
public class CertificationsService {

    @Autowired
    private certificationsRepo repository;

    public List<Map<String, String>> getUniqueEntities() {
        return repository.findDistinctEntities().stream()
                .map(result -> Map.of(
                        "codigo", (String) result[0],
                        "nombre", (String) result[1]))
                .collect(Collectors.toList());
    }

    public List<Map<String, Object>> getRecordsByCodigoEntidad(String codigoEntidad) {
        return repository.findByCodigoEntidad(codigoEntidad).stream()
                .map(result -> Map.of(
                        "fecha", result[0],
                        "porcentajeCalidad", result[1],
                        "estadoCalidad", result[2],
                        "porcentajeL617", result[3],
                        "estadoL617", result[4]))
                .collect(Collectors.toList());
    }

    @Transactional
    public String updateCertification(Map<String, String> requestBody, Long userId, String tipo) {
        LocalDateTime fechaAct = LocalDateTime.now();
        String codigoEntidad = requestBody.get("codigoEntidad");
        int fecha = Integer.parseInt(requestBody.get("fecha"));
        String estado = requestBody.get("estado");
        String observacion = requestBody.get("observacion");

        int updatedRows = 0;

        if ("calidad".equalsIgnoreCase(tipo)) {
            updatedRows = repository.updateCalidad(codigoEntidad, fecha, estado, observacion, fechaAct, userId);
        } else if ("L617".equalsIgnoreCase(tipo)) {
            updatedRows = repository.updateL617(codigoEntidad, fecha, estado, observacion, fechaAct, userId);
        } else {
            throw new IllegalArgumentException("Tipo inválido. Debe ser 'calidad' o 'L617'.");
        }

        if (updatedRows == 0) {
            throw new EntityNotFoundException("No se encontró un registro para actualizar.");
        }

        return "Actualización exitosa.";
    }

}
