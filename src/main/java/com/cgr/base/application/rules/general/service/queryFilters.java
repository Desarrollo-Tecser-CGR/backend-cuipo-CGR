package com.cgr.base.application.rules.general.service;

import java.util.ArrayList;
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

    public List<Map<String, Object>> getReglasGenerales(Integer fecha, String trimestre, String codigoEntidad,
            String ambitoCodigo) {

        List<String> columnasReglaGeneral = obtenerColumnasReglaGeneral();

        if (columnasReglaGeneral.isEmpty()) {
            throw new RuntimeException("No se encontraron columnas que comiencen con 'REGLA_GENERAL_'");
        }

        // Campos adicionales requeridos
        List<String> camposAdicionales = List.of("FECHA", "TRIMESTRE", "NOMBRE_ENTIDAD", "AMBITO_NOMBRE");

        // Combinar columnas de regla general y campos adicionales
        List<String> todasLasColumnas = new ArrayList<>(camposAdicionales);
        todasLasColumnas.addAll(columnasReglaGeneral);

        String columnasSelect = String.join(", ", todasLasColumnas);
        StringBuilder sql = new StringBuilder("SELECT ").append(columnasSelect).append(" FROM ")
                .append(tablaReglas)
                .append(" WHERE 1=1");

        if (fecha != null) {
            sql.append(" AND FECHA = ").append(fecha);
        }
        if (trimestre != null && !trimestre.isEmpty()) {
            sql.append(" AND TRIMESTRE = '").append(trimestre).append("'");
        }
        if (codigoEntidad != null && !codigoEntidad.isEmpty()) {
            sql.append(" AND CODIGO_ENTIDAD = '").append(codigoEntidad).append("'");
        }
        if (ambitoCodigo != null && !ambitoCodigo.isEmpty()) {
            sql.append(" AND AMBITO_CODIGO = '").append(ambitoCodigo).append("'");
        }

        return jdbcTemplate.queryForList(sql.toString());
    }

    private List<String> obtenerColumnasReglaGeneral() {
        String sql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE 'REGLA_GENERAL_%'";
        return jdbcTemplate.queryForList(sql, String.class, tablaReglas);
    }


    // Generar Listado Unico Opciones
    public listOptionsDto getListOptions() {
        return listOptionsDto.builder()
                .fechas(getFechas())
                .trimestres(getTrimestres())
                .entidades(getEntidades())
                .ambitos(getAmbitos())
                .build();
    }
    
    private List<String> getFechas() {
        String sql = "SELECT DISTINCT [FECHA] FROM [cuipo_dev].[dbo].[GENERAL_RULES_DATA] ORDER BY [FECHA]";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    private List<String> getTrimestres() {
        String sql = "SELECT DISTINCT [TRIMESTRE] FROM [cuipo_dev].[dbo].[GENERAL_RULES_DATA] ORDER BY [TRIMESTRE]";
        return jdbcTemplate.queryForList(sql, String.class);
    }
    
    private List<EntidadDTO> getEntidades() {
        String sql = "SELECT DISTINCT [CODIGO_ENTIDAD], [NOMBRE_ENTIDAD] FROM [cuipo_dev].[dbo].[GENERAL_RULES_DATA] ORDER BY [CODIGO_ENTIDAD]";
        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new EntidadDTO(rs.getString("CODIGO_ENTIDAD"), rs.getString("NOMBRE_ENTIDAD")));
    }
    
    private List<AmbitoDTO> getAmbitos() {
        String sql = "SELECT DISTINCT [AMBITO_CODIGO], [AMBITO_NOMBRE] FROM [cuipo_dev].[dbo].[GENERAL_RULES_DATA] ORDER BY [AMBITO_CODIGO]";
        return jdbcTemplate.query(sql, (rs, rowNum) ->
                new AmbitoDTO(rs.getString("AMBITO_CODIGO"), rs.getString("AMBITO_NOMBRE")));
    }

}
