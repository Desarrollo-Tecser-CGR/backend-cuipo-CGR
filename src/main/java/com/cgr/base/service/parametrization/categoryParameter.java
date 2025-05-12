package com.cgr.base.service.parametrization;

import org.springframework.stereotype.Service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

@Service
public class categoryParameter {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private boolean tableExists(String tableName) {
        String checkSql = """
                    SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = ?
                """;
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, tableName);
        return count != null && count > 0;
    }

    public void createYearlyTable(int year) {
        String tableName = "CATEGORIAS_ENTIDADES_" + year;

        if (tableExists(tableName)) {
            throw new IllegalArgumentException("La tabla '" + tableName + "' ya existe.");
        }

        String sql = String.format("""
                    SELECT * INTO %s FROM CATEGORIAS_ENTIDADES
                """, tableName);

        jdbcTemplate.execute(sql);
        System.out.println("Tabla creada: " + tableName);
    }

    public void createRecordForYear(int year, Map<String, Object> requestData) {
        String tableName = "CATEGORIAS_ENTIDADES_" + year;

        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("La tabla '" + tableName + "' no existe.");
        }

        String codigoEntidad = String.valueOf(requestData.get("CODIGO_ENTIDAD"));
        String ambitoCodigo = String.valueOf(requestData.get("AMBITO_CODIGO"));
        String nombreEntidad = String.valueOf(requestData.get("NOMBRE_ENTIDAD")).trim();
        String categoria = String.valueOf(requestData.get("CATEGORIA"));
        Integer noDiputados = (requestData.get("NO_DIPUTADOS") instanceof Integer)
                ? (Integer) requestData.get("NO_DIPUTADOS")
                : Integer.valueOf(String.valueOf(requestData.get("NO_DIPUTADOS")));
        Integer noConcejales = (requestData.get("NO_CONCEJALES") instanceof Integer)
                ? (Integer) requestData.get("NO_CONCEJALES")
                : Integer.valueOf(String.valueOf(requestData.get("NO_CONCEJALES")));

        if (codigoEntidad == null || ambitoCodigo == null || nombreEntidad.isEmpty() || categoria == null
                || noDiputados == null || noConcejales == null) {
            throw new IllegalArgumentException("Todos los campos obligatorios deben estar presentes y no vacíos.");
        }

        String checkSql = String.format("""
                    SELECT COUNT(*) FROM %s WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                """, tableName);

        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, codigoEntidad, ambitoCodigo);
        if (count != null && count > 0) {
            throw new IllegalArgumentException("Ya existe un registro con este CODIGO_ENTIDAD y AMBITO_CODIGO.");
        }

        String nombreFinal = nombreEntidad;
        if (tableExists("SPECIFIC_RULES_DATA")) {
            String nameLookupSql = """
                        SELECT TOP 1 NOMBRE_ENTIDAD FROM SPECIFIC_RULES_DATA
                        WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                    """;
            List<String> names = jdbcTemplate.queryForList(nameLookupSql, String.class, codigoEntidad, ambitoCodigo);
            if (!names.isEmpty() && names.get(0) != null) {
                nombreFinal = names.get(0).toUpperCase();
            }
        }

        String insertSql = String.format(
                """
                            INSERT INTO %s (CODIGO_ENTIDAD, AMBITO_CODIGO, NOMBRE_ENTIDAD, CATEGORIA, NO_DIPUTADOS, NO_CONCEJALES)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                tableName);

        jdbcTemplate.update(insertSql, codigoEntidad, ambitoCodigo, nombreFinal, categoria, noDiputados, noConcejales);
    }

    public void updateRecordForYear(int year, Map<String, Object> requestData) {
        String tableName = "CATEGORIAS_ENTIDADES_" + year;

        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("La tabla '" + tableName + "' no existe.");
        }

        String codigoEntidad = String.valueOf(requestData.get("CODIGO_ENTIDAD"));
        String ambitoCodigo = String.valueOf(requestData.get("AMBITO_CODIGO"));
        String categoria = String.valueOf(requestData.get("CATEGORIA"));
        Integer noDiputados = (requestData.get("NO_DIPUTADOS") instanceof Integer)
                ? (Integer) requestData.get("NO_DIPUTADOS")
                : Integer.valueOf(String.valueOf(requestData.get("NO_DIPUTADOS")));
        Integer noConcejales = (requestData.get("NO_CONCEJALES") instanceof Integer)
                ? (Integer) requestData.get("NO_CONCEJALES")
                : Integer.valueOf(String.valueOf(requestData.get("NO_CONCEJALES")));

        if (codigoEntidad == null || ambitoCodigo == null || categoria == null
                || noDiputados == null || noConcejales == null) {
            throw new IllegalArgumentException("Todos los campos obligatorios deben estar presentes y no vacíos.");
        }

        String checkSql = String.format("""
                    SELECT COUNT(*) FROM %s WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                """, tableName);

        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, codigoEntidad, ambitoCodigo);
        if (count == null || count == 0) {
            throw new IllegalArgumentException(
                    "No existe un registro con el CODIGO_ENTIDAD y AMBITO_CODIGO proporcionados.");
        }

        String updateSql = String.format("""
                    UPDATE %s
                    SET CATEGORIA = ?, NO_DIPUTADOS = ?, NO_CONCEJALES = ?
                    WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                """, tableName);

        jdbcTemplate.update(updateSql, categoria, noDiputados, noConcejales, codigoEntidad, ambitoCodigo);
    }

    public void deleteRecordForYear(int year, String codigoEntidad, String ambitoCodigo) {
        String tableName = "CATEGORIAS_ENTIDADES_" + year;

        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("La tabla '" + tableName + "' no existe.");
        }

        String checkSql = String.format("""
                    SELECT COUNT(*) FROM %s WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                """, tableName);

        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, codigoEntidad, ambitoCodigo);
        if (count == null || count == 0) {
            throw new IllegalArgumentException(
                    "No existe un registro con el CODIGO_ENTIDAD y AMBITO_CODIGO proporcionados.");
        }

        String deleteSql = String.format("""
                    DELETE FROM %s WHERE CODIGO_ENTIDAD = ? AND AMBITO_CODIGO = ?
                """, tableName);

        jdbcTemplate.update(deleteSql, codigoEntidad, ambitoCodigo);
    }

    public List<Map<String, Object>> getAllRecordsByYear(int year) {
        String tableName = "CATEGORIAS_ENTIDADES_" + year;

        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("La tabla '" + tableName + "' no existe.");
        }

        String query = String.format("SELECT * FROM %s", tableName);
        return jdbcTemplate.queryForList(query);
    }

    public List<Integer> getAvailableYears() {
        String sql = """
                    SELECT TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME LIKE 'CATEGORIAS_ENTIDADES_%'
                """;

        List<String> tableNames = jdbcTemplate.queryForList(sql, String.class);

        List<Integer> years = new ArrayList<>();
        for (String tableName : tableNames) {
            try {
                String[] parts = tableName.split("_");
                int year = Integer.parseInt(parts[2]);
                years.add(year);
            } catch (Exception e) {

            }
        }

        Collections.sort(years);
        return years;
    }

}