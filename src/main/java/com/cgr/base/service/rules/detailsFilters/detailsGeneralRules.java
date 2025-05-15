package com.cgr.base.service.rules.detailsFilters;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.parametrization.GeneralRulesNames;
import com.cgr.base.repository.parametrization.generalRulesRepo;

import java.util.*;

@Service
public class detailsGeneralRules {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    generalRulesRepo GeneralRepo;

    public List<Map<String, Object>> getGFInfoDetails() {
        List<GeneralRulesNames> reglas = GeneralRepo.findByDetallesIsNotNullOrderByOrdenAsc();

        List<Map<String, Object>> resultado = new ArrayList<>();

        for (GeneralRulesNames regla : reglas) {
            Map<String, Object> item = new HashMap<>();
            item.put("codigoRegla", regla.getCodigoRegla());
            item.put("detalles", regla.getDetalles());
            resultado.add(item);
        }

        return resultado;
    }

    public List<Map<String, Object>> detailsGeneralRequest(Map<String, String> filters) {

        if (!validateFilters(filters)) {
            return null;
        }

        String fecha = filters.get("fecha");
        String trimestre = filters.get("trimestre");
        String ambito = filters.get("ambito");
        String entidad = filters.get("entidad");
        String codigoRegla = filters.get("codigoRegla");

        String trimestreBD;
        trimestreBD = String.valueOf(Integer.parseInt(trimestre) * 3);

        return getFilteredGeneralData(fecha, trimestreBD, ambito, entidad, codigoRegla);

    }

    public List<Map<String, Object>> getFilteredGeneralData(String fecha, String trimestre, String ambito,
            String entidad, String codigoRegla) {

        Optional<GeneralRulesNames> optionalRegla = GeneralRepo.findByCodigoRegla(codigoRegla);

        if (optionalRegla.isEmpty()) {
            throw new IllegalArgumentException("Código de regla no válido o no encontrado: " + codigoRegla);
        }

        GeneralRulesNames regla = optionalRegla.get();

        if (regla.getDetalles() == null || "0".equals(regla.getDetalles().trim())) {
            throw new IllegalStateException("La regla seleccionada no tiene detalles asociados.");

        }

        String codigoTabla = regla.getCodigoTabla();
        String codigo = regla.getCodigo();
        String nombreTabla = null;

        String checkSql = "SELECT COUNT(*) FROM GENERAL_RULES_TABLES WHERE CODIGO_TABLA = ? AND CODIGO_REGLA = ?";
        Integer count = jdbcTemplate.queryForObject(checkSql, Integer.class, codigoTabla, codigo);

        if (count != null && count > 0) {
            String sql = "SELECT NOMBRE_TABLA FROM GENERAL_RULES_TABLES WHERE CODIGO_TABLA = ? AND CODIGO_REGLA = ?";
            nombreTabla = jdbcTemplate.queryForObject(sql, String.class, codigoTabla, codigo);
        }

        String columnaPatron = "CA00%" + codigo;

        String columnasSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME LIKE ?";

        List<String> columnas = jdbcTemplate.queryForList(columnasSql, String.class, nombreTabla, columnaPatron);

        if (!columnas.isEmpty()) {
            String columnaSeleccionada = columnas.get(0);

            List<String> campos = new ArrayList<>();
            campos.add("CUENTA");
            campos.add("NOMBRE_CUENTA");

            if (regla.getReferencia() != null && !regla.getReferencia().trim().isEmpty()) {
                String[] camposReferencia = regla.getReferencia().split("\\s*,\\s*");
                campos.addAll(Arrays.asList(camposReferencia));
            }

            campos.add(columnaSeleccionada);

            String columnasSeleccionadas = String.join(", ", campos);

            String query = "SELECT " + columnasSeleccionadas +
                    " FROM " + nombreTabla +
                    " WHERE FECHA = ? AND TRIMESTRE = ? AND AMBITO_CODIGO = ? AND CODIGO_ENTIDAD = ? " +
                    " AND " + columnaSeleccionada + " != 'N/A'";

            List<Map<String, Object>> resultados = jdbcTemplate.queryForList(query, fecha, trimestre, ambito, entidad);

            List<Map<String, Object>> resultadosFinales = new ArrayList<>();

            for (Map<String, Object> fila : resultados) {
                Map<String, Object> filaProcesada = new LinkedHashMap<>();

                for (Map.Entry<String, Object> entry : fila.entrySet()) {
                    String clave = entry.getKey();
                    Object valor = entry.getValue();

                    if (clave.equals(columnaSeleccionada)) {

                        if ("1".equals(String.valueOf(valor)) || "0".equals(String.valueOf(valor))) {
                            filaProcesada.put("ESTADO", valor);
                        } else {
                            filaProcesada.put("ESTADO", "SIN DATOS");
                        }
                    } else {
                        filaProcesada.put(clave, valor);
                    }
                }

                resultadosFinales.add(filaProcesada);
            }

            return resultadosFinales;

        } else {
            throw new IllegalStateException("No hay información disponible para mostrar en este momento.");

        }
    }

    private boolean validateFilters(Map<String, String> filters) {
        return filters.containsKey("fecha") &&
                filters.containsKey("trimestre") &&
                filters.containsKey("ambito") &&
                filters.containsKey("entidad") &&
                filters.containsKey("codigoRegla") &&
                !filters.get("fecha").isEmpty() &&
                !filters.get("trimestre").isEmpty() &&
                !filters.get("ambito").isEmpty() &&
                !filters.get("entidad").isEmpty() &&
                !filters.get("codigoRegla").isEmpty();
    }

}
