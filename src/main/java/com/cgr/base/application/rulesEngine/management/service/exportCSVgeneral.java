package com.cgr.base.application.rulesEngine.management.service;

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

import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG;

@Service
public class exportCSVgeneral {

    private final queryFilters queryFilters;
    private final listOptionsRG listOptions;

    public exportCSVgeneral(queryFilters queryFilters) {
        this.queryFilters = queryFilters;
        this.listOptions = queryFilters.getListOptionsGenerals();
    }

    public ByteArrayOutputStream generateCsvStream(Map<String, String> filters) throws IOException {
        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambitoCodigo = filters.get("ambito");
        String entidadCodigo = filters.get("entidad");
        String formularioCodigo = filters.get("formulario");

        String trimestreBD = (trimestre != null) ? convertirTrimestreParaBD(trimestre) : null;

        List<Map<String, Object>> filteredData = queryFilters.getFilteredRecordsGR(fecha, trimestreBD, ambitoCodigo,
                entidadCodigo, formularioCodigo);

        List<Map<String, Object>> filteredDataFormatted = filteredData.stream()
                .map(row -> {
                    row.put("TRIMESTRE", convertirTrimestreParaFront(row.get("TRIMESTRE")));
                    return row;
                })
                .toList();

        String ambitoNombre = listOptions.getAmbitos().stream()
                .filter(a -> a.getCodigo().equals(ambitoCodigo))
                .map(listOptionsRG.AmbitoDTO::getNombre)
                .findFirst()
                .orElse(null);

        String entidadNombre = listOptions.getEntidades().stream()
                .filter(e -> e.getCodigo().equals(entidadCodigo))
                .map(listOptionsRG.EntidadDTO::getNombre)
                .findFirst()
                .orElse(null);

        String formularioNombre = listOptions.getFormularios().stream()
                .filter(f -> f.getCodigo().equals(formularioCodigo))
                .map(listOptionsRG.FormularioDTO::getNombre)
                .findFirst()
                .orElse(null);

        ZoneId colombiaZone = ZoneId.of("America/Bogota");
        String fechaHoraGeneracion = ZonedDateTime.now(colombiaZone)
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

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

            csvPrinter.printRecord("Reporte generado el:", fechaHoraGeneracion);
            csvPrinter.printRecord("Filtros Aplicados:");
            if (fecha != null)
                csvPrinter.printRecord("Fecha", fecha);
            if (trimestre != null)
                csvPrinter.printRecord("Trimestre", convertirTrimestreParaFront(trimestre));
            if (ambitoCodigo != null && ambitoNombre != null)
                csvPrinter.printRecord("Ámbito", ambitoCodigo + " - " + ambitoNombre);
            if (entidadCodigo != null && entidadNombre != null)
                csvPrinter.printRecord("Entidad", entidadCodigo + " - " + entidadNombre);
            if (formularioCodigo != null && formularioNombre != null)
                csvPrinter.printRecord("Formulario", formularioCodigo + " - " + formularioNombre);
            csvPrinter.println();

            if (!filteredDataFormatted.isEmpty()) {
                csvPrinter.printRecord(filteredDataFormatted.get(0).keySet());
            }

            for (Map<String, Object> record : filteredDataFormatted) {
                csvPrinter.printRecord(record.values());
            }

            writer.flush();
            csvPrinter.flush();
            return byteArrayOutputStream;
        }
    }

    private String convertirTrimestreParaBD(String trimestre) {
        return switch (trimestre) {
            case "1" -> "3";
            case "2" -> "6";
            case "3" -> "9";
            case "4" -> "12";
            default -> trimestre;
        };
    }

    private String convertirTrimestreParaFront(Object trimestreBD) {
        if (trimestreBD == null)
            return null;

        return switch (trimestreBD.toString()) {
            case "3" -> "1";
            case "6" -> "2";
            case "9" -> "3";
            case "12" -> "4";
            default -> trimestreBD.toString();
        };
    }

}
