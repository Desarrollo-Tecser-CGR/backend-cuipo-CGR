package com.cgr.base.application.rulesEngine.alertsRules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class alertsRules {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    // Obtener alertas de GENERAL_RULES_DATA
    public List<Map<String, Object>> getFilteredAlertsGR(Map<String, String> filters) {
        return getFilteredAlerts("GENERAL_RULES_DATA", filters);
    }

    // Obtener alertas de SPECIFIC_RULES_DATA
    public List<Map<String, Object>> getFilteredAlertsSR(Map<String, String> filters) {
        return getFilteredAlerts("SPECIFIC_RULES_DATA", filters);
    }

    // Función genérica para filtrar alertas en cualquier tabla
    private List<Map<String, Object>> getFilteredAlerts(String tableName, Map<String, String> filters) {
        if (!tablaExiste(tableName)) {
            return List.of();
        }

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String entidadCodigo = filters != null ? filters.get("entidad") : null;

        String trimestreBD = null;
        if (trimestre != null) {
            try {
                int trimestreInt = Integer.parseInt(trimestre);
                trimestreBD = String.valueOf(trimestreInt * 3);
            } catch (NumberFormatException e) {
                System.err.println("Error al convertir trimestre: " + trimestre);
            }
        }

        List<String> columnasAlerta = obtenerColumnasAlerta(tableName);
        if (columnasAlerta.isEmpty()) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasAlerta)).append(" FROM ").append(tableName).append(" WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (fecha != null) {
            try {
                int fechaInt = Integer.parseInt(fecha);
                sql.append(" AND FECHA = :fecha");
                params.put("fecha", fechaInt);
            } catch (NumberFormatException e) {
                System.err.println("Error al convertir fecha: " + fecha);
            }
        }

        if (trimestreBD != null) {
            sql.append(" AND TRIMESTRE = :trimestre");
            params.put("trimestre", trimestreBD);
        }

        if (entidadCodigo != null && !entidadCodigo.isBlank()) {
            sql.append(" AND CODIGO_ENTIDAD = :entidad");
            params.put("entidad", entidadCodigo);
        }

        return namedParameterJdbcTemplate.queryForList(sql.toString(), params);
    }

    // Obtener columnas de alerta
    private List<String> obtenerColumnasAlerta(String tabla) {
        String sql = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                    AND COLUMN_NAME LIKE 'ALERTA_%'
                """;
        return jdbcTemplate.queryForList(sql, String.class, tabla);
    }

    // Verificar si la tabla existe
    private boolean tablaExiste(String tabla) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tabla);
        return count != null && count > 0;
    }

}
