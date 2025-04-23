package com.cgr.base.service.rules;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.dto.rules.listOptionsEG;
import com.cgr.base.dto.rules.listOptionsRG;
import com.cgr.base.dto.rules.listOptionsRG.AmbitoDTO;
import com.cgr.base.dto.rules.listOptionsRG.EntidadDTO;
import com.cgr.base.dto.rules.listOptionsRG.FormularioDTO;

@Service
public class queryFilters {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private boolean tablaExiste(String tabla) {
        String sql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?";
        Integer count = jdbcTemplate.queryForObject(sql, Integer.class, tabla);
        return count != null && count > 0;
    }

    public listOptionsRG getListOptionsGenerals() {
        if (!tablaExiste("GENERAL_RULES_DATA")) {
            return listOptionsRG.builder().build();
        }
        return listOptionsRG.builder()
                .fechas(getFechas("GENERAL_RULES_DATA"))
                .trimestres(convertirTrimestres(getTrimestres("GENERAL_RULES_DATA")))
                .entidades(getEntidades("GENERAL_RULES_DATA"))
                .ambitos(getAmbitos("GENERAL_RULES_DATA"))
                .formularios(getFormTables())
                .build();
    }

    public listOptionsEG getListOptionsSpecific() {
        if (!tablaExiste("SPECIFIC_RULES_DATA")) {
            return listOptionsEG.builder().build();
        }
        return listOptionsEG.builder()
                .fechas(getFechas("SPECIFIC_RULES_DATA"))
                .trimestres(convertirTrimestres(getTrimestres("SPECIFIC_RULES_DATA")))
                .entidades(getEntidades("SPECIFIC_RULES_DATA"))
                .ambitos(getAmbitos("SPECIFIC_RULES_DATA"))
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

    public List<Map<String, Object>> getFilteredRecordsGR(Map<String, String> filters) {
        if (!tablaExiste("GENERAL_RULES_DATA"))
            return List.of();

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String ambitoCodigo = filters != null ? filters.get("ambito") : null;
        String entidadCodigo = filters != null ? filters.get("entidad") : null;
        String formularioCodigo = filters != null ? filters.get("formulario") : null;

        String trimestreBD = (trimestre != null) ? String.valueOf(Integer.parseInt(trimestre) * 3) : null;

        StringBuilder sql = new StringBuilder("SELECT FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_NOMBRE");
        List<String> columnasReglaGeneral = obtenerColumnasReglaGeneral(formularioCodigo);

        if (!columnasReglaGeneral.isEmpty()) {
            sql.append(", ").append(String.join(", ", columnasReglaGeneral));
        }

        sql.append(" FROM ").append("GENERAL_RULES_DATA").append(" WHERE 1=1");

        if (fecha != null) {
            sql.append(" AND FECHA = ").append(Integer.parseInt(fecha));
        }
        if (trimestreBD != null) {
            sql.append(" AND TRIMESTRE = ").append(Integer.parseInt(trimestreBD));
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
                .map(row -> {
                    if (row.containsKey("TRIMESTRE")) {
                        row.put("TRIMESTRE", Integer.parseInt(row.get("TRIMESTRE").toString()) / 3);
                    }
                    return cambiarNombresRG(row, mapaColumnas);
                })
                .toList();
    }

    public List<Map<String, Object>> getFilteredRecordsSR(Map<String, String> filters) {
        if (filters == null) {
            filters = new HashMap<>();
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambitoCodigo = filters.get("ambito");
        String entidadCodigo = filters.get("entidad");
        String reporteCodigo = filters.get("reporte");

        // Convertir trimestre para la base de datos
        String trimestreBD = (trimestre != null) ? String.valueOf(Integer.parseInt(trimestre) * 3) : null;

        String tablaConsulta;

        if (reporteCodigo == null || reporteCodigo.trim().isEmpty()) {
            tablaConsulta = "SPECIFIC_RULES_DATA";
        } else {
            tablaConsulta = obtenerTablaDesdeCodigo(reporteCodigo);
        }

        if (tablaConsulta == null || !tablaExiste(tablaConsulta)) {
            return List.of();
        }

        List<String> columnasValidas = obtenerColumnasValidas(tablaConsulta);

        if (columnasValidas.isEmpty()) {
            return List.of();
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasValidas)).append(" FROM ").append(tablaConsulta).append(" WHERE 1=1");

        if (fecha != null) {
            sql.append(" AND FECHA = ").append(Integer.parseInt(fecha));
        }
        if (trimestreBD != null) {
            sql.append(" AND TRIMESTRE = ").append(Integer.parseInt(trimestreBD));
        }
        if (ambitoCodigo != null) {
            sql.append(" AND AMBITO_CODIGO = '").append(ambitoCodigo).append("'");
        }
        if (entidadCodigo != null) {
            sql.append(" AND CODIGO_ENTIDAD = '").append(entidadCodigo).append("'");
        }

        List<Map<String, Object>> result = jdbcTemplate.queryForList(sql.toString());

        Map<String, String> mapaColumnas = obtenerNombresSR(columnasValidas);

        return result.stream()
                .map(row -> {
                    if (row.containsKey("TRIMESTRE")) {
                        int trimestreBDValue = Integer.parseInt(row.get("TRIMESTRE").toString());
                        row.put("TRIMESTRE", trimestreBDValue / 3);
                    }
                    return cambiarNombresSR(row, mapaColumnas);
                })
                .toList();

    }

    private Map<String, String> obtenerNombresSR(List<String> codigosRegla) {
        if (codigosRegla.isEmpty()) {
            return Map.of();
        }

        String sql = "SELECT CODIGO_REGLA, NOMBRE_REGLA FROM SPECIFIC_RULES_NAMES WHERE CODIGO_REGLA IN (" +
                codigosRegla.stream().map(c -> "'" + c + "'").collect(Collectors.joining(", ")) + ")";

        List<Map<String, Object>> resultados = jdbcTemplate.queryForList(sql);

        return resultados.stream()
                .collect(Collectors.toMap(
                        r -> (String) r.get("CODIGO_REGLA"),
                        r -> (String) r.get("NOMBRE_REGLA")));
    }

    private Map<String, Object> cambiarNombresSR(Map<String, Object> row, Map<String, String> mapaColumnas) {
        Map<String, Object> nuevaFila = new LinkedHashMap<>();

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            String nombreColumna = entry.getKey();
            String nuevoNombre = mapaColumnas.getOrDefault(nombreColumna, nombreColumna);
            nuevaFila.put(nuevoNombre, entry.getValue());
        }

        return nuevaFila;
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
        if (!tablaExiste("GENERAL_RULES_DATA")) {
            return List.of();
        }

        if (codigoFormulario == null || codigoFormulario.isEmpty()) {
            String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'";
            return jdbcTemplate.queryForList(sql, String.class, "GENERAL_RULES_DATA");
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
        return jdbcTemplate.queryForList(sql, String.class, "GENERAL_RULES_DATA");
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

    public Map<String, String> processLastUpdateRequestG(Map<String, String> request) {
        Integer fecha = parseInteger(request.get("fecha"));
        Integer trimestre = parseInteger(request.get("trimestre"));

        if (fecha == null || trimestre == null) {
            return null;
        }

        Integer trimestreConvertido = convertirTrimestre(trimestre);
        if (trimestreConvertido == null) {
            return null;
        }

        String lastUpdate = getLastUpdateDateGR(fecha, trimestreConvertido);
        Map<String, String> data = new HashMap<>();
        data.put("GENERAL_RULES_DATA", lastUpdate != null ? lastUpdate : "NO DATA");

        return data;
    }

    public Map<String, String> processLastUpdateRequestE(Map<String, String> request) {
        Integer fecha = parseInteger(request.get("fecha"));
        Integer trimestre = parseInteger(request.get("trimestre"));

        if (fecha == null || trimestre == null) {
            return null;
        }

        Integer trimestreConvertido = convertirTrimestre(trimestre);
        if (trimestreConvertido == null) {
            return null;
        }

        String lastUpdate = getLastUpdateDateSR(fecha, trimestreConvertido);
        Map<String, String> data = new HashMap<>();
        data.put("SPECIFIC_RULES_DATA", lastUpdate != null ? lastUpdate : "NO DATA");

        return data;
    }

    private Integer parseInteger(String value) {
        try {
            return value != null ? Integer.valueOf(value) : null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private Integer convertirTrimestre(Integer trimestre) {
        return switch (trimestre) {
            case 1 -> 3;
            case 2 -> 6;
            case 3 -> 9;
            case 4 -> 12;
            default -> null;
        };
    }

    public String getLastUpdateDateGR(Integer fecha, Integer trimestre) {
        if (!tablaExiste("GENERAL_RULES_DATA") || fecha == null || trimestre == null) {
            return null;
        }

        String sql = "SELECT MAX(FECHA_CARGUE) FROM " + "GENERAL_RULES_DATA" +
                " WHERE FECHA = ? AND TRIMESTRE = ?";

        return jdbcTemplate.queryForObject(sql, String.class, fecha, trimestre);
    }

    public String getLastUpdateDateSR(Integer fecha, Integer trimestre) {
        if (!tablaExiste("SPECIFIC_RULES_DATA") || fecha == null || trimestre == null) {
            return null;
        }

        String sql = "SELECT MAX(FECHA_CARGUE) FROM " + "SPECIFIC_RULES_DATA" +
                " WHERE FECHA = ? AND TRIMESTRE = ?";

        return jdbcTemplate.queryForObject(sql, String.class, fecha, trimestre);
    }

}