package com.cgr.base.service.rules.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataBaseUtils {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Transactional
    public void ensureColumnsExist(String tableName, String... columns) {
        // Consulta para obtener nombre, tipo y longitud de cada columna existente
        String query = String.format(
                "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH " +
                        "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'",
                tableName);

        List<Object[]> existing = entityManager.createNativeQuery(query).getResultList();

        // Map de columnas existentes con tipo y longitud
        Map<String, ColumnInfo> existingColumnMap = new HashMap<>();
        for (Object[] row : existing) {
            String name = row[0].toString();
            String type = row[1].toString().toUpperCase();
            Integer length = row[2] != null ? ((Number) row[2]).intValue() : null;
            existingColumnMap.put(name, new ColumnInfo(type, length));
        }

        StringBuilder alterTableQuery = new StringBuilder();

        for (String column : columns) {
            String[] parts = column.split(":");
            String columnName = parts[0];
            String desiredFullType = (parts.length > 1) ? parts[1].toUpperCase() : "VARCHAR(255)";
            String desiredType = desiredFullType.split("\\(")[0];
            Integer desiredLength = extractLength(desiredFullType);

            if (!existingColumnMap.containsKey(columnName)) {
                // No existe la columna: agregarla
                alterTableQuery.append(
                        String.format("ALTER TABLE %s ADD %s %s NULL; ", tableName, columnName, desiredFullType));
            } else {
                ColumnInfo existingInfo = existingColumnMap.get(columnName);
                boolean typeMismatch = !existingInfo.type.equalsIgnoreCase(desiredType);
                boolean lengthMismatch = (desiredLength != null && !desiredLength.equals(existingInfo.length));

                if (typeMismatch || lengthMismatch) {
                    // Diferencias en tipo o longitud: eliminar y crear nuevamente
                    alterTableQuery.append(String.format("ALTER TABLE %s DROP COLUMN %s; ", tableName, columnName));
                    alterTableQuery.append(
                            String.format("ALTER TABLE %s ADD %s %s NULL; ", tableName, columnName, desiredFullType));
                }
            }
        }

        if (alterTableQuery.length() > 0) {
            entityManager.createNativeQuery(alterTableQuery.toString()).executeUpdate();
        }
    }

    // Clase auxiliar para guardar info de columnas
    private static class ColumnInfo {
        String type;
        Integer length;

        ColumnInfo(String type, Integer length) {
            this.type = type;
            this.length = length;
        }
    }

    // Extraer longitud de tipos como VARCHAR(255)
    private static Integer extractLength(String typeString) {
        if (typeString.contains("(") && typeString.contains(")")) {
            try {
                String inside = typeString.substring(typeString.indexOf('(') + 1, typeString.indexOf(')'));
                return Integer.parseInt(inside.trim());
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }

    public List<String> obtenerColumnasDeTabla(String tabla) {
        String sql = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                """;
        return jdbcTemplate.queryForList(sql, String.class, tabla);
    }

    public boolean tablaExiste(String tabla) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tabla);
        return count != null && count > 0;
    }

}
