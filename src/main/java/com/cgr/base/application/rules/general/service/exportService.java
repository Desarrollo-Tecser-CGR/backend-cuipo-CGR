package com.cgr.base.application.rules.general.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rules.general.dto.listOptionsDto;

@Service
public class exportService {

    private final queryFilters queryFilters;
    private final listOptionsDto listOptions;

    public exportService(queryFilters queryFilters) {
        this.queryFilters = queryFilters;
        this.listOptions = new listOptionsDto();
    }

    public ByteArrayOutputStream generateCsvStream(Map<String, String> filters) throws IOException {
        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambitoCodigo = filters.get("ambito");
        String entidadCodigo = filters.get("entidad");
        String formularioCodigo = filters.get("formulario");

        // Obtener los datos filtrados
        List<Map<String, Object>> filteredData = queryFilters.getFilteredRecords(fecha, trimestre, ambitoCodigo, entidadCodigo, formularioCodigo);

        // Obtener nombres de ámbito y entidad
        String ambitoNombre = listOptions.getAmbitos() != null ? listOptions.getAmbitos().stream()
                .filter(a -> a.getCodigo().equals(ambitoCodigo))
                .map(listOptionsDto.AmbitoDTO::getNombre)
                .findFirst()
                .orElse("Desconocido") : "Desconocido";
        
        String entidadNombre = listOptions.getEntidades() != null ? listOptions.getEntidades().stream()
                .filter(e -> e.getCodigo().equals(entidadCodigo))
                .map(listOptionsDto.EntidadDTO::getNombre)
                .findFirst()
                .orElse("Desconocido") : "Desconocido";

        // Obtener fecha y hora actual en zona horaria de Colombia
        ZoneId colombiaZone = ZoneId.of("America/Bogota");
        String fechaHoraGeneracion = ZonedDateTime.now(colombiaZone).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
            .setQuoteMode(QuoteMode.ALL_NON_NULL)
            .setDelimiter(',')
            .setQuote('"')
            .setEscape('\\')
            .setNullString("")
            .build();

        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             OutputStreamWriter writer = new OutputStreamWriter(byteArrayOutputStream);
             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

            // Escribir filtros como bloque inicial
            csvPrinter.printRecord("Reporte generado el:", fechaHoraGeneracion);
            csvPrinter.printRecord("Filtros Aplicados:");
            csvPrinter.printRecord("Fecha", fecha);
            csvPrinter.printRecord("Trimestre", trimestre);
            csvPrinter.printRecord("Ámbito", ambitoCodigo + " - " + ambitoNombre);
            csvPrinter.printRecord("Entidad", entidadCodigo + " - " + entidadNombre);
            csvPrinter.printRecord("Formulario", formularioCodigo);
            csvPrinter.println();

            // Obtener encabezados dinámicos
            if (!filteredData.isEmpty()) {
                csvPrinter.printRecord(filteredData.get(0).keySet());
            }

            // Escribir los datos
            for (Map<String, Object> record : filteredData) {
                csvPrinter.printRecord(record.values());
            }

            writer.flush();
            csvPrinter.flush();
            return byteArrayOutputStream;
        }
    }
    
}
