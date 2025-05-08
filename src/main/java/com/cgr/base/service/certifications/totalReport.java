package com.cgr.base.service.certifications;

import java.util.*;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

@Service
public class totalReport {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    private static final List<ComplianceLevel> COMPLIANCE_LEVELS = buildComplianceLevels();

    private static List<ComplianceLevel> buildComplianceLevels() {
        List<ComplianceLevel> levels = new ArrayList<>();

        // Definimos cortes y nombres
        List<String> names = Arrays.asList("BAJO", "MEDIO", "ALTO", "EXCELENTE");
        List<Double> cuts = Arrays.asList(0.0, 40.0, 60.0, 80.0, 100.0);

        for (int i = 0; i < names.size(); i++) {
            double min = cuts.get(i);
            double max = (i == names.size() - 1) ? cuts.get(i + 1) : cuts.get(i + 1) - 0.01;
            levels.add(new ComplianceLevel(names.get(i), min, max));
        }

        return levels;
    }

    public List<Map<String, Object>> getCertificationStats() {
        String sql = "SELECT " +
                "TRY_CAST(FECHA AS INT) AS FECHA, " +
                "TRY_CAST(PORCENTAJE_CALIDAD AS DECIMAL(5,2)) AS PORCENTAJE_CALIDAD, " +
                "TRY_CAST(PORCENTAJE_L617 AS DECIMAL(5,2)) AS PORCENTAJE_L617 " +
                "FROM " + DATASOURCE_NAME + ".dbo.CONTROL_CERTIFICACION " +
                "WHERE FECHA IS NOT NULL";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        Map<Integer, Map<String, Map<String, Integer>>> aggregatedData = new HashMap<>();

        for (Object[] row : results) {
            Integer year = toInteger(row[0]);
            Double porcentajeCalidad = toDouble(row[1]);
            Double porcentajeL617 = toDouble(row[2]);

            if (year == null)
                continue;

            aggregatedData.putIfAbsent(year, new HashMap<>());

            Map<String, Map<String, Integer>> yearData = aggregatedData.get(year);
            yearData.putIfAbsent("CALIDAD", new HashMap<>());
            yearData.putIfAbsent("L617", new HashMap<>());

            String calidadLevel = getComplianceLevel(porcentajeCalidad);
            String l617Level = getComplianceLevel(porcentajeL617);

            yearData.get("CALIDAD").merge(calidadLevel, 1, Integer::sum);
            yearData.get("L617").merge(l617Level, 1, Integer::sum);
        }

        // Preparar la lista final
        List<Map<String, Object>> dataList = new ArrayList<>();

        for (Map.Entry<Integer, Map<String, Map<String, Integer>>> entry : aggregatedData.entrySet()) {
            Map<String, Object> yearData = new HashMap<>();
            yearData.put("FECHA", entry.getKey());
            yearData.put("CALIDAD", new HashMap<>(entry.getValue().getOrDefault("CALIDAD", new HashMap<>())));
            yearData.put("L617", new HashMap<>(entry.getValue().getOrDefault("L617", new HashMap<>())));
            dataList.add(yearData);
        }

        // Armar etiquetas dinámicamente
        Map<String, String> etiquetas = new HashMap<>();
        for (ComplianceLevel level : COMPLIANCE_LEVELS) {
            int minRounded = (int) level.getMin();
            int maxRounded = (int) Math.floor(level.getMax());
            etiquetas.put(
                    level.getName(),
                    String.format("%d%% - %d%%", minRounded, maxRounded));
        }

        // Armar respuesta final
        Map<String, Object> result = new HashMap<>();
        result.put("informacion", dataList);
        result.put("etiquetas", etiquetas);

        List<Map<String, Object>> response = new ArrayList<>();
        response.add(result);

        return response;
    }

    private String getComplianceLevel(Double percentage) {
        if (percentage == null) {
            return "BAJO";
        }
        for (ComplianceLevel level : COMPLIANCE_LEVELS) {
            if (percentage >= level.getMin() && percentage <= level.getMax()) {
                return level.getName();
            }
        }
        return "DESCONOCIDO";
    }

    private Integer toInteger(Object value) {
        if (value == null)
            return null;
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to Integer: " + value);
    }

    private Double toDouble(Object value) {
        if (value == null)
            return 0.0;
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        throw new IllegalArgumentException("Cannot convert to Double: " + value);
    }

    // Clase interna para definir los niveles de cumplimiento
    public static class ComplianceLevel {
        private String name;
        private double min;
        private double max;

        public ComplianceLevel(String name, double min, double max) {
            this.name = name;
            this.min = min;
            this.max = max;
        }

        public String getName() {
            return name;
        }

        public double getMin() {
            return min;
        }

        public double getMax() {
            return max;
        }
    }
}
