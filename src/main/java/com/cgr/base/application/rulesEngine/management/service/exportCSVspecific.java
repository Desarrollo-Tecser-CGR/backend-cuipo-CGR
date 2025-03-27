package com.cgr.base.application.rulesEngine.management.service;

import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.management.dto.listOptionsEG;

@Service
public class exportCSVspecific {

    private final queryFilters queryFilters;
    private final listOptionsEG listOptions;

    public exportCSVspecific(queryFilters queryFilters) {
        this.queryFilters = queryFilters;
        this.listOptions = queryFilters.getListOptionsSpecific();
    }

    // public ByteArrayOutputStream generateCsvStream(Map<String, String> filters) throws IOException {
    //     String fecha = filters.get("fecha");
    //     String trimestre = filters.get("trimestre");
    //     String ambitoCodigo = filters.get("ambito");
    //     String entidadCodigo = filters.get("entidad");
    //     String reporteCodigo = filters.get("reporte");

    //     String trimestreBD = (trimestre != null) ? convertirTrimestreParaBD(trimestre) : null;

    //     List<Map<String, Object>> filteredData = queryFilters.getFilteredRecordsSR(fecha, trimestreBD, ambitoCodigo,
    //             entidadCodigo, reporteCodigo);

    //     List<Map<String, Object>> filteredDataFormatted = filteredData.stream()
    //             .map(row -> {
    //                 Map<String, Object> updatedRow = new LinkedHashMap<>();

    //                 for (Map.Entry<String, Object> entry : row.entrySet()) {
    //                     String columnName = entry.getKey();
    //                     Object value = entry.getValue();

    //                     if (columnName.matches("^CA0\\d{3,}$") || columnName.startsWith("REGLA_ESPECIFICA_")) {
    //                         updatedRow.put("RESULTADO_REPORTE", value);
    //                     } else {
    //                         updatedRow.put(columnName, value);
    //                     }
    //                 }

    //                 if (updatedRow.containsKey("TRIMESTRE")) {
    //                     updatedRow.put("TRIMESTRE", convertirTrimestreParaFront(updatedRow.get("TRIMESTRE")));
    //                 }

    //                 return updatedRow;
    //             })
    //             .toList();

    //     String ambitoNombre = listOptions.getAmbitos().stream()
    //             .filter(a -> a.getCodigo().equals(ambitoCodigo))
    //             .map(a -> a.getNombre())
    //             .findFirst()
    //             .orElse(null);

    //     String entidadNombre = listOptions.getEntidades().stream()
    //             .filter(e -> e.getCodigo().equals(entidadCodigo))
    //             .map(e -> e.getNombre())
    //             .findFirst()
    //             .orElse(null);

    //     String reporteNombre = listOptions.getReportes().stream()
    //             .filter(r -> r.getCodigo().equals(reporteCodigo))
    //             .map(r -> r.getNombre())
    //             .findFirst()
    //             .orElse(null);

    //     ZoneId colombiaZone = ZoneId.of("America/Bogota");
    //     String fechaHoraGeneracion = ZonedDateTime.now(colombiaZone)
    //             .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    //     CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
    //             .setQuoteMode(QuoteMode.ALL_NON_NULL)
    //             .setDelimiter(',')
    //             .setQuote('"')
    //             .setEscape('\\')
    //             .setNullString("")
    //             .build();

    //     try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    //             OutputStreamWriter writer = new OutputStreamWriter(byteArrayOutputStream);
    //             CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

    //         csvPrinter.printRecord("Reporte generado el:", fechaHoraGeneracion);
    //         csvPrinter.printRecord("Filtros Aplicados:");
    //         if (fecha != null)
    //             csvPrinter.printRecord("Fecha", fecha);
    //         if (trimestre != null)
    //             csvPrinter.printRecord("Trimestre", convertirTrimestreParaFront(trimestre));
    //         if (ambitoCodigo != null && ambitoNombre != null)
    //             csvPrinter.printRecord("Ámbito", ambitoCodigo + " - " + ambitoNombre);
    //         if (entidadCodigo != null && entidadNombre != null)
    //             csvPrinter.printRecord("Entidad", entidadCodigo + " - " + entidadNombre);
    //         if (reporteCodigo != null && reporteNombre != null)
    //             csvPrinter.printRecord("Reporte", reporteCodigo + " - " + reporteNombre);
    //         csvPrinter.println();

    //         if (!filteredDataFormatted.isEmpty()) {
    //             csvPrinter.printRecord(filteredDataFormatted.get(0).keySet());
    //         }

    //         for (Map<String, Object> record : filteredDataFormatted) {
    //             csvPrinter.printRecord(record.values());
    //         }

    //         writer.flush();
    //         csvPrinter.flush();
    //         return byteArrayOutputStream;
    //     }
    // }

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
