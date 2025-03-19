package com.cgr.base.application.rulesEngine.utils;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class dataBaseUtils {

    @PersistenceContext
    private EntityManager entityManager;

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

}
