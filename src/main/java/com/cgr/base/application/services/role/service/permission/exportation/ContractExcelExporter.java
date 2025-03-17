package com.cgr.base.application.services.role.service.permission.exportation;
/*
import com.cgr.base.application.services.role.service.permission.servicesPemission.Contractor.RepositoryContractor;
import com.cgr.base.domain.models.entity.EntityContractor;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

@Service
public class ContractExcelExporter {

    @Autowired
    RepositoryContractor repositoryContractor;

    public byte[] downloadContractsExcel() throws IOException {
        List<EntityContractor> contracts = repositoryContractor.findAll();
        return generateExcel(contracts);
    }

    public byte[] generateExcel(List<EntityContractor> contracts) throws IOException {
        try (HSSFWorkbook workbook = new HSSFWorkbook(); ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            Sheet sheet = workbook.createSheet("Contratos");
            int rowNum = 0;

            // Cabecera
            Row headerRow = sheet.createRow(rowNum++);
            Cell headerCell1 = headerRow.createCell(0);
            headerCell1.setCellValue("Número de Contrato");
            Cell headerCell2 = headerRow.createCell(1);
            headerCell2.setCellValue("Valor Inicial del Contrato");
            Cell headerCell3 = headerRow.createCell(2);
            headerCell3.setCellValue("Fecha de Suscripción");
            Cell headerCell4 = headerRow.createCell(3);
            headerCell4.setCellValue("Fecha de Inicio de Registro");
            Cell headerCell5 = headerRow.createCell(4);
            headerCell5.setCellValue("Término Inicial del Contrato");
            Cell headerCell6 = headerRow.createCell(5);
            headerCell6.setCellValue("Fecha de Fin del Contrato");

            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

            for (EntityContractor contract : contracts) {
                Row row = sheet.createRow(rowNum++);
                Cell cell1 = row.createCell(0);
                cell1.setCellValue(contract.getContractor_number());
                Cell cell2 = row.createCell(1);
                cell2.setCellValue(contract.getInitial_contract_value());
                Cell cell3 = row.createCell(2);
                if (contract.getSuscription_date() != null) {
                    cell3.setCellValue(dateFormat.format(contract.getSuscription_date()));
                }
                Cell cell4 = row.createCell(3);
                if (contract.getStart_record_date() != null) {
                    cell4.setCellValue(dateFormat.format(contract.getStart_record_date()));
                }
                Cell cell5 = row.createCell(4);
                cell5.setCellValue(contract.getInitial_contract_term());
                Cell cell6 = row.createCell(5);
                if (contract.getContract_end_date() != null) {
                    cell6.setCellValue(dateFormat.format(contract.getContract_end_date()));
                }
            }

            workbook.write(outputStream);
            return outputStream.toByteArray();
        }
    }


}    */