package com.cgr.base.service.rules.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
        String query = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'", tableName);

        @SuppressWarnings("unchecked")
        List<String> existingColumns = (List<String>) entityManager.createNativeQuery(query).getResultList()
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        Set<String> existingColumnSet = Set.copyOf(existingColumns);

        StringBuilder alterTableQuery = new StringBuilder();
        for (String column : columns) {
            String[] parts = column.split(":");
            String columnName = parts[0];
            String columnType = (parts.length > 1) ? parts[1] : "VARCHAR(255)";

            if (!existingColumnSet.contains(columnName)) {
                alterTableQuery.append(
                        String.format("ALTER TABLE %s ADD %s %s NULL; ", tableName, columnName, columnType));
            }
        }

        if (alterTableQuery.length() > 0) {
            entityManager.createNativeQuery(alterTableQuery.toString()).executeUpdate();
        }
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
