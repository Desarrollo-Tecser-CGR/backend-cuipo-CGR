package com.cgr.base.application.rulesEngine.specificRules;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class detailsInfo {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> getFilteredRecordsGF(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo) {
        String tablaConsulta = "[cuipo_dev].[dbo].[VW_OPENDATA_D_EJECUCION_GASTOS]";

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

    public List<Map<String, Object>> getFilteredRecordsICLD(String fecha, String trimestre, String ambitoCodigo,
            String entidadCodigo) {
        String tablaConsulta = "[cuipo_dev].[dbo].[VW_OPENDATA_B_EJECUCION_INGRESOS]";

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
