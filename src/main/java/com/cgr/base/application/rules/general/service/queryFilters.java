package com.cgr.base.application.rules.general.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.application.rules.general.dto.listOptionsDto;
import com.cgr.base.application.rules.general.dto.listOptionsDto.AmbitoDTO;
import com.cgr.base.application.rules.general.dto.listOptionsDto.EntidadDTO;

@Service
public class queryFilters {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_GENERAL_RULES}")
    private String tablaReglas;

    // Generar Listado Unico Opciones.
    public listOptionsDto getListOptions() {
        return listOptionsDto.builder()
                .fechas(getFechas())
                .trimestres(getTrimestres())
                .entidades(getEntidades())
                .ambitos(getAmbitos())
                .build();
    }
    
    private List<String> getFechas() {
        String sql = String.format("SELECT DISTINCT [FECHA] FROM [%s] ORDER BY [FECHA]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    private List<String> getTrimestres() {
        String sql = String.format("SELECT DISTINCT [TRIMESTRE] FROM [%s] ORDER BY [TRIMESTRE]", tablaReglas);
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    private List<EntidadDTO> getEntidades() {
        String sql = String.format("SELECT DISTINCT [CODIGO_ENTIDAD], [NOMBRE_ENTIDAD] FROM [%s] ORDER BY [CODIGO_ENTIDAD]", tablaReglas);
        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new EntidadDTO(rs.getString("CODIGO_ENTIDAD"), rs.getString("NOMBRE_ENTIDAD")));
    }
    
    private List<AmbitoDTO> getAmbitos() {
        String sql = String.format("SELECT DISTINCT [AMBITO_CODIGO], [AMBITO_NOMBRE] FROM [%s] ORDER BY [AMBITO_CODIGO]", tablaReglas);
        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new AmbitoDTO(rs.getString("AMBITO_CODIGO"), rs.getString("AMBITO_NOMBRE")));
    }

    private List<String> obtenerColumnasReglaGeneral() {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'";
        return jdbcTemplate.queryForList(sql, String.class, tablaReglas);
    }

    public List<Map<String, Object>> getFilteredRecords(String fecha, String trimestre, String ambitoCodigo, String entidadCodigo) {

        List<String> columnasReglaGeneral = obtenerColumnasReglaGeneral();

        StringBuilder sql = new StringBuilder("SELECT FECHA, TRIMESTRE, NOMBRE_ENTIDAD, AMBITO_NOMBRE");

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


    

}
