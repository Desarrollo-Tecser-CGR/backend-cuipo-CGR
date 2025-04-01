package com.cgr.base.application.rulesEngine.specificRules;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class detailsInfo {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    public List<Map<String, Object>> processGFRequest(Map<String, String> filters) {
        if (!validateFilters(filters)) {
            return null;
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambito = filters.get("ambito");
        String entidad = filters.get("entidad");

        String trimestreBD = String.valueOf(Integer.parseInt(trimestre) * 3);

        return getFilteredRecordsGF(fecha, trimestreBD, ambito, entidad);
    }

    public List<Map<String, Object>> getFilteredRecordsGF(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo) {
        String tablaConsulta = "[" + DATASOURCE_NAME + "].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS]";

        List<String> columnasValidas = List.of(
                "CUENTA", "NOMBRE_CUENTA",
                "COD_VIGENCIA_DEL_GASTO", "NOM_VIGENCIA_DEL_GASTO",
                "COD_SECCION_PRESUPUESTAL", "NOM_SECCION_PRESUPUESTAL",
                "COD_FUENTES_FINANCIACION", "NOM_FUENTES_FINANCIACION",
                "COMPROMISOS", "REGLA_25_A AS GF");

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasValidas))
                .append(" FROM ").append(tablaConsulta)
                .append(" WHERE 1=1");

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

    public List<Map<String, Object>> processICLDRequest(Map<String, String> filters) {
        if (!validateFilters(filters)) {
            return null;
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambito = filters.get("ambito");
        String entidad = filters.get("entidad");

        String trimestreBD = String.valueOf(Integer.parseInt(trimestre) * 3);

        return getFilteredRecordsICLD(fecha, trimestreBD, ambito, entidad);
    }

    private boolean validateFilters(Map<String, String> filters) {
        return filters != null &&
                filters.containsKey("fecha") &&
                filters.containsKey("trimestre") &&
                filters.containsKey("ambito") &&
                filters.containsKey("entidad");
    }

    public List<Map<String, Object>> getFilteredRecordsICLD(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo) {
        String tablaConsulta = "[" + DATASOURCE_NAME + "].[dbo].[VW_OPENDATA_B_EJECUCION_INGRESOS]";

        List<String> columnasValidas = List.of(
                "CUENTA", "NOMBRE_CUENTA",
                "COD_FUENTES_FINANCIACION", "NOM_FUENTES_FINANCIACION",
                "TOTAL_RECAUDO", "REGLA_22_A AS ICLD");

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasValidas))
                .append(" FROM ").append(tablaConsulta)
                .append(" WHERE 1=1");

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

}
