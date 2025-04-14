package com.cgr.base.service.rules;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.springframework.stereotype.Service;

import com.cgr.base.dto.rules.listOptionsEG;

@Service
public class exportCSVspecific {

    private final queryFilters queryFilters;
    private final listOptionsEG listOptions;

    public exportCSVspecific(queryFilters queryFilters) {
        this.queryFilters = queryFilters;
        this.listOptions = queryFilters.getListOptionsSpecific();
    }

    public ByteArrayOutputStream generateCsvStream(Map<String, String> filters) throws IOException {
        if (filters == null) {
            filters = new HashMap<>();
        }

        List<Map<String, Object>> filteredData = queryFilters.getFilteredRecordsSR(filters);

        String ambitoCodigo = filters.get("ambito");
        String entidadCodigo = filters.get("entidad");
        String reporteCodigo = filters.get("reporte");

        String ambitoNombre = listOptions.getAmbitos().stream()
                .filter(a -> a.getCodigo().equals(ambitoCodigo))
                .map(a -> a.getNombre())
                .findFirst()
                .orElse(null);

        String entidadNombre = listOptions.getEntidades().stream()
                .filter(e -> e.getCodigo().equals(entidadCodigo))
                .map(e -> e.getNombre())
                .findFirst()
                .orElse(null);

        String reporteNombre = listOptions.getReportes().stream()
                .filter(r -> r.getCodigo().equals(reporteCodigo))
                .map(r -> r.getNombre())
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
            if (filters.get("fecha") != null)
                csvPrinter.printRecord("Fecha", filters.get("fecha"));
            if (filters.get("trimestre") != null)
                csvPrinter.printRecord("Trimestre", filters.get("trimestre"));
            if (ambitoCodigo != null && ambitoNombre != null)
                csvPrinter.printRecord("Ámbito", ambitoCodigo + " - " + ambitoNombre);
            if (entidadCodigo != null && entidadNombre != null)
                csvPrinter.printRecord("Entidad", entidadCodigo + " - " + entidadNombre);
            if (reporteCodigo != null && reporteNombre != null)
                csvPrinter.printRecord("Reporte", reporteCodigo + " - " + reporteNombre);
            csvPrinter.println();

            if (!filteredData.isEmpty()) {
                csvPrinter.printRecord(filteredData.get(0).keySet());
            }

            for (Map<String, Object> record : filteredData) {
                csvPrinter.printRecord(record.values());
            }

            writer.flush();
            csvPrinter.flush();
            return byteArrayOutputStream;
        }
    }

}
