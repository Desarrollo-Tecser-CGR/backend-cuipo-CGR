package com.cgr.base.application.services.role.service;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Service
public class ExportService {

    @Autowired
    private RepositoryNotification repositoryNotification;

    public Path generateExcel() throws IOException {
        Path filePath = Paths.get(System.getProperty("user.home"), "notifications.xlsx");
        Files.createDirectories(filePath.getParent());

        try (FileOutputStream fos = new FileOutputStream(filePath.toString());
             Workbook workbook = new XSSFWorkbook()) {

            Sheet sheet = workbook.createSheet("Notificaciones");

            // Crear encabezados
            Row headerRow = sheet.createRow(0);
            String[] headers = {"ID", "Notificación", "Sujeto", "Fecha", "Número de contrato", "Usuario", "Cargo"};
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
            }

            // Obtener datos y llenar las filas
            List<EntityNotification> notifications = repositoryNotification.findAll();
            int rowNum = 1;
            for (EntityNotification notification : notifications) {
                Row row = sheet.createRow(rowNum++);
                row.createCell(0).setCellValue(notification.getId());
                row.createCell(1).setCellValue(notification.getNotification() != null ? notification.getNotification() : "N/A");
                row.createCell(2).setCellValue(notification.getSubject() != null ? notification.getSubject() : "N/A");
                row.createCell(3).setCellValue(notification.getDate() != null ? notification.getDate().toString() : "N/A");
                row.createCell(4).setCellValue(notification.getNumbercontract() != null ? notification.getNumbercontract() : "N/A");
                row.createCell(5).setCellValue(notification.getUser() != null ? notification.getUser().getFullName() : "N/A");
                row.createCell(6).setCellValue(notification.getUser() != null ? notification.getUser().getCargo() : "N/A");
            }

            // Ajustar el ancho de las columnas automáticamente
            for (int i = 0; i < headers.length; i++) {
                sheet.autoSizeColumn(i);
            }

            workbook.write(fos);

            System.out.println("✅ Excel guardado correctamente en: " + filePath);
            return filePath;

        }
    }
}