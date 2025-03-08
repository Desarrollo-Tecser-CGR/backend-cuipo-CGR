package com.cgr.base.application.rulesEngine.management.service;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.management.dto.listOptionsDto;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsDto.AmbitoDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsDto.EntidadDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsDto.FormularioDTO;

@Service
public class queryFilters {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    private boolean tablaExiste(String tabla) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tabla);
        return count != null && count > 0;
    }

    public listOptionsDto getListOptions() {
        if (!tablaExiste(tablaReglas)) {
            return listOptionsDto.builder().build();
        }
        return listOptionsDto.builder()
                .fechas(getFechas())
                .trimestres(getTrimestres())
                .entidades(getEntidades())
                .ambitos(getAmbitos())
                .formularios(getFormTables())
                .build();
    }

    private List<String> getFechas() {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format("SELECT DISTINCT [FECHA] FROM [%s] ORDER BY [FECHA]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }

    private List<FormularioDTO> getFormTables() {
        String sql = "SELECT DISTINCT [CODIGO_TABLA], [NOMBRE_TABLA] FROM general_rules_tables ORDER BY [CODIGO_TABLA]";
        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new FormularioDTO(rs.getString("CODIGO_TABLA"), rs.getString("NOMBRE_TABLA")));
    }

    private List<String> getTrimestres() {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format("SELECT DISTINCT [TRIMESTRE] FROM [%s] ORDER BY [TRIMESTRE]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }

    private List<EntidadDTO> getEntidades() {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format(
                "SELECT DISTINCT [CODIGO_ENTIDAD], [NOMBRE_ENTIDAD], [AMBITO_CODIGO] FROM [%s] ORDER BY [CODIGO_ENTIDAD]",
                tablaReglas);
        return jdbcTemplate.query(sql, (rs, rowNum) -> new EntidadDTO(rs.getString("CODIGO_ENTIDAD"),
                rs.getString("NOMBRE_ENTIDAD"), rs.getString("AMBITO_CODIGO")));
    }

    private List<AmbitoDTO> getAmbitos() {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format(
                "SELECT DISTINCT [AMBITO_CODIGO], [AMBITO_NOMBRE] FROM [%s] ORDER BY [AMBITO_CODIGO]", tablaReglas);
        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new AmbitoDTO(rs.getString("AMBITO_CODIGO"), rs.getString("AMBITO_NOMBRE")));
    }

    public List<Map<String, Object>> getFilteredRecords(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo, String formularioCodigo) {

        if (!tablaExiste(tablaReglas))
            return List.of();

        StringBuilder sql = new StringBuilder("SELECT FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_NOMBRE");
        List<String> columnasReglaGeneral = obtenerColumnasReglaGeneral(formularioCodigo);

        if (!columnasReglaGeneral.isEmpty()) {
            sql.append(", ").append(String.join(", ", columnasReglaGeneral));
        }

        sql.append(" FROM ").append(tablaReglas).append(" WHERE 1=1");

        if (fecha != null) {
            sql.append(" AND FECHA = ").append(Integer.parseInt(fecha));
        }
        if (trimestre != null) {
            sql.append(" AND TRIMESTRE = ").append(Integer.parseInt(trimestre));
        }
        if (ambitoCodigo != null) {
            sql.append(" AND AMBITO_CODIGO = '").append(ambitoCodigo).append("'");
        }
        if (entidadCodigo != null) {
            sql.append(" AND CODIGO_ENTIDAD = '").append(entidadCodigo).append("'");
        }

        return jdbcTemplate.queryForList(sql.toString());
    }

    private List<String> obtenerColumnasReglaGeneral(String codigoFormulario) {
        if (!tablaExiste(tablaReglas))
            return List.of();

        if (codigoFormulario == null || codigoFormulario.isEmpty()) {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'";
            return jdbcTemplate.queryForList(sql, String.class, tablaReglas);
        }

        String sqlRegla = "SELECT DISTINCT NOMBRE_REGLA FROM general_rules_tables WHERE CODIGO_TABLA = ?";
        List<String> nombresRegla = jdbcTemplate.queryForList(sqlRegla, String.class, codigoFormulario);

        if (nombresRegla.isEmpty()) {
            return obtenerColumnasReglaGeneral(null);
        }

        String condition = nombresRegla.stream()
                .map(nombre -> "COLUMN_NAME = '" + nombre + "'")
                .collect(Collectors.joining(" OR "));

        String sql = String.format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND (%s)",
                condition);
        return jdbcTemplate.queryForList(sql, String.class, tablaReglas);
    }
}