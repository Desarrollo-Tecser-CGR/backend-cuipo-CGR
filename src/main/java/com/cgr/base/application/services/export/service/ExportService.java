package com.cgr.base.application.services.export.service;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ExportService {

    @Autowired
    private RepositoryNotification repositoryNotification;

    public String generateCSV() {
        StringBuilder csvBuilder = new StringBuilder();

        // Crear encabezados
        String[] headers = { "ID", "Notificación", "Sujeto", "Fecha", "Número de contrato", "Usuario", "Cargo" };
        csvBuilder.append(String.join(",", headers)).append("\n");

        // Obtener datos y llenar las filas
        List<EntityNotification> notifications = repositoryNotification.findAll();
        for (EntityNotification notification : notifications) {
            csvBuilder.append(notification.getId()).append(",");
            csvBuilder.append(notification.getNotification() != null ? notification.getNotification() : "N/A")
                    .append(",");
            csvBuilder.append(notification.getSubject() != null ? notification.getSubject() : "N/A").append(",");
            csvBuilder.append(notification.getDate() != null ? notification.getDate().toString() : "N/A").append(",");
            csvBuilder.append(notification.getNumbercontract() != null ? notification.getNumbercontract() : "N/A")
                    .append(",");
            csvBuilder.append(notification.getUser() != null ? notification.getUser().getFullName() : "N/A")
                    .append(",");
            csvBuilder.append(notification.getUser() != null ? notification.getUser().getCargo() : "N/A").append("\n");
        }

        return csvBuilder.toString();
    }
}