package com.cgr.base.application.services.export.service;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.ExportCount;
import com.cgr.base.infrastructure.repositories.repositories.RepositoryExportCount;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.util.IOUtils;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Service;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Date;

@Service
public class ExportService {

    @Autowired
    private RepositoryNotification repositoryNotification;
    @Autowired
    private RepositoryExportCount repositoryExportCount;
    @Autowired
    private ResourceLoader resourceLoader; // Para cargar recursos desde el classpath

    public String generateCSV() {
        StringBuilder csvBuilder = new StringBuilder();
        String[] headers = { "ID", "Notificación", "Sujeto", "Fecha", "Número de contrato", "Usuario", "Cargo" };
        csvBuilder.append(String.join(",", headers)).append("\n");
        List<EntityNotification> notifications = repositoryNotification.findAll();
        for (EntityNotification notification : notifications) {
            csvBuilder.append(notification.getId()).append(",");
            csvBuilder.append(notification.getNotification() != null ? notification.getNotification() : "N/A").append(",");
            csvBuilder.append(notification.getSubject() != null ? notification.getSubject() : "N/A").append(",");
            csvBuilder.append(notification.getDate() != null ? notification.getDate().toString() : "N/A").append(",");
            csvBuilder.append(notification.getNumbercontract() != null ? notification.getNumbercontract() : "N/A").append(",");
            csvBuilder.append(notification.getUser() != null ? notification.getUser().getFullName() : "N/A").append(",");
            csvBuilder.append(notification.getUser() != null ? notification.getUser().getCargo() : "N/A").append("\n");
        }
        return csvBuilder.toString();
    }

    public String generateExcelWithStyle() throws IOException {
        Workbook workbook = new XSSFWorkbook();
        Sheet sheet = workbook.createSheet("Notificaciones");

        // **Estilos**

        // Estilo para la fila de encabezados
        Font headerFont = workbook.createFont();
        headerFont.setBold(true);
        headerFont.setColor(IndexedColors.WHITE.getIndex());
        CellStyle headerStyle = workbook.createCellStyle();
        headerStyle.setFont(headerFont);
        headerStyle.setFillForegroundColor(IndexedColors.DARK_BLUE.getIndex());
        headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
        headerStyle.setAlignment(HorizontalAlignment.CENTER);
        headerStyle.setBorderBottom(BorderStyle.THIN);
        headerStyle.setBorderTop(BorderStyle.THIN);
        headerStyle.setBorderLeft(BorderStyle.THIN);
        headerStyle.setBorderRight(BorderStyle.THIN);

        // Estilo para las filas de datos
        Font dataFont = workbook.createFont();
        dataFont.setColor(IndexedColors.BLACK.getIndex());
        CellStyle dataStyle = workbook.createCellStyle();
        dataStyle.setFont(dataFont);
        dataStyle.setBorderBottom(BorderStyle.THIN);
        dataStyle.setBorderTop(BorderStyle.THIN);
        dataStyle.setBorderLeft(BorderStyle.THIN);
        dataStyle.setBorderRight(BorderStyle.THIN);

        // Estilo para filas de datos alternas
        CellStyle dataStyleAlt = workbook.createCellStyle();
        dataStyleAlt.setFont(dataFont);
        dataStyleAlt.setBorderBottom(BorderStyle.THIN);
        dataStyleAlt.setBorderTop(BorderStyle.THIN);
        dataStyleAlt.setBorderLeft(BorderStyle.THIN);
        dataStyleAlt.setBorderRight(BorderStyle.THIN);
        dataStyleAlt.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
        dataStyleAlt.setFillPattern(FillPatternType.SOLID_FOREGROUND);

        // Estilo para celdas de fecha
        CreationHelper createHelper = workbook.getCreationHelper();
        CellStyle dateStyle = workbook.createCellStyle();
        dateStyle.setDataFormat(createHelper.createDataFormat().getFormat("yyyy-MM-dd HH:mm:ss"));
        dateStyle.setBorderBottom(BorderStyle.THIN);
        dateStyle.setBorderTop(BorderStyle.THIN);
        dateStyle.setBorderLeft(BorderStyle.THIN);
        dateStyle.setBorderRight(BorderStyle.THIN);

        // **Insertar Logo**
        try (InputStream inputStream = resourceLoader.getResource("classpath:static/imagenesMaps/ContrExcel.png").getInputStream()) {
            byte[] bytes = IOUtils.toByteArray(inputStream);
            int pictureIdx = workbook.addPicture(bytes, Workbook.PICTURE_TYPE_PNG);
            CreationHelper helper = workbook.getCreationHelper();
            Drawing drawing = sheet.createDrawingPatriarch();
            ClientAnchor anchor = helper.createClientAnchor();
            anchor.setCol1(0);
            anchor.setRow1(0);
            anchor.setCol2(2); // Ajusta según el ancho de tu logo
            anchor.setRow2(3); // Ajusta según la altura de tu logo
            Picture picture = drawing.createPicture(anchor, pictureIdx);
            picture.resize();
        } catch (IOException e) {
            System.err.println("Error al cargar la imagen del logo: " + e.getMessage());
        }

        // **Crear Encabezados**
        int headerRowNum = 3; // Fila 4
        Row headerRow = sheet.createRow(headerRowNum);
        String[] headers = { "ID", "Notificación", "Sujeto", "Fecha", "Número de contrato", "Usuario", "Cargo" };
        for (int i = 0; i < headers.length; i++) {
            Cell cell = headerRow.createCell(i);
            cell.setCellValue(headers[i]);
            cell.setCellStyle(headerStyle);
        }

        // **Obtener datos y llenar las filas**
        List<EntityNotification> notifications = repositoryNotification.findAll();
        int rowNum = headerRowNum + 1;
        for (int i = 0; i < notifications.size(); i++) {
            EntityNotification notification = notifications.get(i);
            Row row = sheet.createRow(rowNum++);
            CellStyle currentRowStyle = (i % 2 == 0) ? dataStyleAlt : dataStyle;

            Cell idCell = row.createCell(0);
            idCell.setCellValue(notification.getId());
            idCell.setCellStyle(currentRowStyle);

            Cell notificationCell = row.createCell(1);
            notificationCell.setCellValue(notification.getNotification() != null ? notification.getNotification() : "N/A");
            notificationCell.setCellStyle(currentRowStyle);

            Cell subjectCell = row.createCell(2);
            subjectCell.setCellValue(notification.getSubject() != null ? notification.getSubject() : "N/A");
            subjectCell.setCellStyle(currentRowStyle);

            Cell dateCell = row.createCell(3);
            if (notification.getDate() != null) {
                dateCell.setCellValue(notification.getDate());
                dateCell.setCellStyle(dateStyle);
            } else {
                dateCell.setCellValue("N/A");
                dateCell.setCellStyle(currentRowStyle);
            }

            Cell contractCell = row.createCell(4);
            contractCell.setCellValue(notification.getNumbercontract() != null ? notification.getNumbercontract() : "N/A");
            contractCell.setCellStyle(currentRowStyle);

            Cell userCell = row.createCell(5);
            userCell.setCellValue(notification.getUser() != null ? notification.getUser().getFullName() : "N/A");
            userCell.setCellStyle(currentRowStyle);

            Cell cargoCell = row.createCell(6);
            cargoCell.setCellValue(notification.getUser() != null ? notification.getUser().getCargo() : "N/A");
            cargoCell.setCellStyle(currentRowStyle);
        }

        // **Ajustar el ancho de las columnas automáticamente**
        for (int i = 0; i < headers.length; i++) {
            sheet.autoSizeColumn(i);
        }

        // **Guardar el archivo Excel**
        String filename = "notificaciones_con_diseno.xlsx";
        try (FileOutputStream outputStream = new FileOutputStream(filename)) {
            workbook.write(outputStream);
        }
        workbook.close();

        return filename;
    }

    public void incrementExportCount(String exportType) {
        ExportCount exportCount = new ExportCount();
        exportCount.setExportDate(new Date());
        exportCount.setExportCount(1);
        exportCount.setExportType("CSV"); // Correcto: solo un argumento
        repositoryExportCount.save(exportCount);
    }

    public long getTotalExportCount() {
        return repositoryExportCount.count();
    }

    public List<Object[]> getDistinctMonthsAndYears() {
        return repositoryExportCount.findDistinctMonthsAndYears();
    }

    public List<Object[]> getExportCountsByMonthAndYear() {
        return repositoryExportCount.countExportsByMonthAndYear();
    }
}