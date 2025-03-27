package com.cgr.base.application.rulesEngine.management.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rulesEngine.management.dto.listOptionsEG;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.AmbitoDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.EntidadDTO;
import com.cgr.base.application.rulesEngine.management.dto.listOptionsRG.FormularioDTO;

@Service
public class queryFilters {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaGenerales;

    @Value("${TABLA_SPECIFIC_RULES}")
    private String tablaEspecificas;

    private boolean tablaExiste(String tabla) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tabla);
        return count != null && count > 0;
    }

    public listOptionsRG getListOptionsGenerals() {
        if (!tablaExiste(tablaGenerales)) {
            return listOptionsRG.builder().build();
        }
        return listOptionsRG.builder()
                .fechas(getFechas(tablaGenerales))
                .trimestres(convertirTrimestres(getTrimestres(tablaGenerales)))
                .entidades(getEntidades(tablaGenerales))
                .ambitos(getAmbitos(tablaGenerales))
                .formularios(getFormTables())
                .build();
    }

    public listOptionsEG getListOptionsSpecific() {
        if (!tablaExiste(tablaEspecificas)) {
            return listOptionsEG.builder().build();
        }
        return listOptionsEG.builder()
                .fechas(getFechas(tablaEspecificas))
                .trimestres(convertirTrimestres(getTrimestres(tablaEspecificas)))
                .entidades(getEntidades(tablaEspecificas))
                .ambitos(getAmbitos(tablaEspecificas))
                .reportes(getFormReports())
                .build();
    }

    private List<String> convertirTrimestres(List<String> trimestresBD) {
        if (trimestresBD == null || trimestresBD.isEmpty()) {
            return List.of();
        }
        return trimestresBD.stream()
                .map(Integer::parseInt)
                .map(t -> t / 3)
                .map(String::valueOf)
                .collect(Collectors.toList());
    }

    private List<String> getFechas(String tablaReglas) {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format("SELECT DISTINCT [FECHA] FROM [%s] ORDER BY [FECHA]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }

    private List<FormularioDTO> getFormTables() {
        if (!tablaExiste("GENERAL_RULES_TABLES")) {
            return List.of();
        }
        String sql = "SELECT DISTINCT [CODIGO_TABLA], [NOMBRE_TABLA] FROM GENERAL_RULES_TABLES ORDER BY [CODIGO_TABLA]";
        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new FormularioDTO(rs.getString("CODIGO_TABLA"), rs.getString("NOMBRE_TABLA")));
    }

    private List<FormularioDTO> getFormReports() {
        if (!tablaExiste("SPECIFIC_RULES_TABLES")) {
            return List.of();
        }
        String sql = "SELECT DISTINCT [CODIGO_REPORTE], [NOMBRE_REPORTE] FROM SPECIFIC_RULES_TABLES ORDER BY [CODIGO_REPORTE]";
        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new FormularioDTO(rs.getString("CODIGO_REPORTE"), rs.getString("NOMBRE_REPORTE")));
    }

    private List<String> getTrimestres(String tablaReglas) {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format("SELECT DISTINCT [TRIMESTRE] FROM [%s] ORDER BY [TRIMESTRE]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }

    private List<EntidadDTO> getEntidades(String tablaReglas) {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format(
                "SELECT DISTINCT [CODIGO_ENTIDAD], [NOMBRE_ENTIDAD], [AMBITO_CODIGO] FROM [%s] ORDER BY [CODIGO_ENTIDAD]",
                tablaReglas);
        return jdbcTemplate.query(sql, (rs, rowNum) -> new EntidadDTO(rs.getString("CODIGO_ENTIDAD"),
                rs.getString("NOMBRE_ENTIDAD"), rs.getString("AMBITO_CODIGO")));
    }

    private List<AmbitoDTO> getAmbitos(String tablaReglas) {
        if (!tablaExiste(tablaReglas))
            return List.of();
        String sql = String.format(
                "SELECT DISTINCT [AMBITO_CODIGO], [AMBITO_NOMBRE] FROM [%s] ORDER BY [AMBITO_CODIGO]", tablaReglas);
        return jdbcTemplate.query(sql,
                (rs, rowNum) -> new AmbitoDTO(rs.getString("AMBITO_CODIGO"), rs.getString("AMBITO_NOMBRE")));
    }

    public List<Map<String, Object>> getFilteredRecordsGR(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo, String formularioCodigo) {

        if (!tablaExiste(tablaGenerales))
            return List.of();

        StringBuilder sql = new StringBuilder("SELECT FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_NOMBRE");
        List<String> columnasReglaGeneral = obtenerColumnasReglaGeneral(formularioCodigo);

        if (!columnasReglaGeneral.isEmpty()) {
            sql.append(", ").append(String.join(", ", columnasReglaGeneral));
        }

        sql.append(" FROM ").append(tablaGenerales).append(" WHERE 1=1");

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

        List<Map<String, Object>> resultados = jdbcTemplate.queryForList(sql.toString());

        Map<String, String> mapaColumnas = obtenerNombresRG(columnasReglaGeneral);

        return resultados.stream()
                .map(row -> cambiarNombresRG(row, mapaColumnas))
                .toList();
    }

    public List<Map<String, Object>> getFilteredRecordsSR(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo, String reporteCodigo) {

        String tablaConsulta;
        
        if (reporteCodigo == null || reporteCodigo.trim().isEmpty()) {
            tablaConsulta = tablaEspecificas;
        } else {
            tablaConsulta = obtenerTablaDesdeCodigo(reporteCodigo);
        }

        if (tablaConsulta == null || !tablaExiste(tablaConsulta)) {
            return List.of();
        }

        // Obtener columnas válidas
        List<String> columnasValidas = obtenerColumnasValidas(tablaConsulta);

        if (columnasValidas.isEmpty()) {
            return List.of();
        }

        // Construcción de la consulta SQL
        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasValidas)).append(" FROM ").append(tablaConsulta).append(" WHERE 1=1");

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

    private String obtenerTablaDesdeCodigo(String reporteCodigo) {
        if (reporteCodigo == null) {
            return null;
        }

        String sql = "SELECT NOMBRE_TABLA FROM SPECIFIC_RULES_TABLES WHERE CODIGO_REPORTE = ?";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, reporteCodigo);

        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    private List<String> obtenerColumnasValidas(String tabla) {
        String sql = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                    AND COLUMN_NAME NOT LIKE 'ALERTA_%'
                    AND COLUMN_NAME <> 'FECHA_CARGUE'
                """;
        return jdbcTemplate.queryForList(sql, String.class, tabla);
    }

    private List<String> obtenerColumnasReglaGeneral(String codigoFormulario) {
        if (!tablaExiste(tablaGenerales)) {
            return List.of();
        }

        if (codigoFormulario == null || codigoFormulario.isEmpty()) {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'";
            return jdbcTemplate.queryForList(sql, String.class, tablaGenerales);
        }

        if (!tablaExiste("GENERAL_RULES_TABLES")) {
            return List.of();
        }

        String sqlRegla = "SELECT DISTINCT NOMBRE_REGLA FROM GENERAL_RULES_TABLES WHERE CODIGO_TABLA = ?";
        List<String> nombresRegla = jdbcTemplate.queryForList(sqlRegla, String.class, codigoFormulario);

        if (nombresRegla.isEmpty()) {
            return obtenerColumnasReglaGeneral(null);
        }

        String condition = nombresRegla.stream()
                .map(nombre -> "COLUMN_NAME = '" + nombre + "'")
                .collect(Collectors.joining(" OR "));

        String sql = String.format("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND (%s)",
                condition);
        return jdbcTemplate.queryForList(sql, String.class, tablaGenerales);
    }

    private Map<String, String> obtenerNombresRG(List<String> codigosRegla) {
        if (codigosRegla.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT CODIGO_REGLA, NOMBRE_REGLA FROM GENERAL_RULES_NAMES WHERE CODIGO_REGLA IN ("
                + codigosRegla.stream().map(c -> "'" + c + "'").collect(Collectors.joining(", ")) + ")";

        List<Map<String, Object>> resultados = jdbcTemplate.queryForList(sql);

        return resultados.stream()
                .collect(Collectors.toMap(
                        r -> (String) r.get("CODIGO_REGLA"),
                        r -> (String) r.get("NOMBRE_REGLA")));
    }

    private Map<String, Object> cambiarNombresRG(Map<String, Object> row, Map<String, String> mapaColumnas) {
        Map<String, Object> nuevaFila = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String nombreColumna = entry.getKey();

            String nuevoNombre = mapaColumnas.getOrDefault(nombreColumna, nombreColumna);
            nuevaFila.put(nuevoNombre, entry.getValue());
        }

        return nuevaFila;
    }

    public String getLastUpdateDateGR(Integer fecha, Integer trimestre) {
        if (!tablaExiste(tablaGenerales) || fecha == null || trimestre == null) {
            return null;
        }

        String sql = "SELECT MAX(FECHA_CARGUE) FROM " + tablaGenerales +
                " WHERE FECHA = ? AND TRIMESTRE = ?";

        return jdbcTemplate.queryForObject(sql, String.class, fecha, trimestre);
    }

    public String getLastUpdateDateSR(Integer fecha, Integer trimestre) {
        if (!tablaExiste(tablaEspecificas) || fecha == null || trimestre == null) {
            return null;
        }

        String sql = "SELECT MAX(FECHA_CARGUE) FROM " + tablaEspecificas +
                " WHERE FECHA = ? AND TRIMESTRE = ?";

        return jdbcTemplate.queryForObject(sql, String.class, fecha, trimestre);
    }

}