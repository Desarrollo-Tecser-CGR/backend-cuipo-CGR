package com.cgr.base.service.rules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class alertsRules {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @Autowired
    private dataBaseUtils UtilsDB;

    // Obtener listado de ALERTAS de GENERAL_RULES_DATA
    public List<Map<String, Object>> getFilteredAlertsGR(Map<String, String> filters) {
        return getFilteredAlerts("GENERAL_RULES_DATA", filters);
    }

    // Obtener listado de ALERTAS de SPECIFIC_RULES_DATA
    public List<Map<String, Object>> getFilteredAlertsSR(Map<String, String> filters) {
        return getFilteredAlerts("SPECIFIC_RULES_DATA", filters);
    }

    // Obtener ALERTAS desde la tabla especificada aplicando filtros.
    private List<Map<String, Object>> getFilteredAlerts(String tableName, Map<String, String> filters) {
        if (!UtilsDB.tablaExiste(tableName)) {
            return List.of();
        }

        String fecha = filters != null ? filters.get("fecha") : null;
        String trimestre = filters != null ? filters.get("trimestre") : null;
        String entidadCodigo = filters != null ? filters.get("entidad") : null;

        String trimestreBD = null;
        if (trimestre != null) {
            try {
                int trimestreInt = Integer.parseInt(trimestre);
                trimestreBD = String.valueOf(trimestreInt * 3);
            } catch (NumberFormatException e) {
                System.err.println("⚠️ Error al convertir trimestre: " + trimestre);
            }
        }

        List<String> columnasAlerta = obtenerColumnasAlerta(tableName);
        if (columnasAlerta.isEmpty()) {
            return List.of();
        }

        Set<String> columnasQuery = new HashSet<>(columnasAlerta);
        Map<String, Map<String, String>> referenciaAlertas = obtenerTablaRef();
        Set<String> columnasExistentes = new HashSet<>(UtilsDB.obtenerColumnasDeTabla(tableName));

        for (Map<String, String> ref : referenciaAlertas.values()) {
            String columnRef = ref.get("column_ref");
            if (!"0".equals(columnRef) && columnasExistentes.contains(columnRef)) {
                columnasQuery.add(columnRef); // solo se agrega si existe en la tabla
            }
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(String.join(", ", columnasQuery)).append(" FROM ").append(tableName).append(" WHERE 1=1");

        Map<String, Object> params = new HashMap<>();

        if (fecha != null) {
            try {
                int fechaInt = Integer.parseInt(fecha);
                sql.append(" AND FECHA = :fecha");
                params.put("fecha", fechaInt);
            } catch (NumberFormatException e) {
                System.err.println("⚠️ Error al convertir fecha: " + fecha);
            }
        }

        if (trimestreBD != null) {
            sql.append(" AND TRIMESTRE = :trimestre");
            params.put("trimestre", trimestreBD);
        }

        if (entidadCodigo != null && !entidadCodigo.isBlank()) {
            sql.append(" AND CODIGO_ENTIDAD = :entidad");
            params.put("entidad", entidadCodigo);
        }
        List<Map<String, Object>> resultados = namedParameterJdbcTemplate.queryForList(sql.toString(), params);

        for (Map<String, Object> fila : resultados) {
            List<String> mensajesUnicos = new ArrayList<>();
            Set<String> columnasUsadasEnAlertas = new HashSet<>();

            for (String columna : columnasAlerta) {
                Object valor = fila.get(columna);
                if (valor != null) {
                    String textoAlerta = valor.toString().trim();
                    if ("OK".equalsIgnoreCase(textoAlerta))
                        continue;

                    String mensajeFinal = textoAlerta;
                    Map<String, String> referencia = referenciaAlertas.get(textoAlerta);

                    if (referencia != null) {
                        String message = referencia.get("message");
                        String columnRef = referencia.get("column_ref");

                        if (!"0".equals(columnRef) && fila.containsKey(columnRef)) {
                            Object extra = fila.get(columnRef);
                            if (extra != null) {
                                message += " " + limpiarNombreColumna(columnRef) + ": " + extra + ".";
                                columnasUsadasEnAlertas.add(columnRef);
                            }
                        }

                        mensajeFinal = message;
                    }

                    if (!mensajesUnicos.contains(mensajeFinal)) {
                        mensajesUnicos.add(mensajeFinal);
                    }
                }
            }

            for (String columna : columnasAlerta) {
                fila.remove(columna);
            }

            for (String col : columnasQuery) {
                if (!columnasAlerta.contains(col) && !columnasUsadasEnAlertas.contains(col)) {
                    fila.remove(col);
                } else if (!columnasAlerta.contains(col)) {
                    fila.remove(col);
                }
            }

            for (int i = 0; i < mensajesUnicos.size(); i++) {
                fila.put(String.valueOf(i + 1), mensajesUnicos.get(i));
            }
        }

        return resultados;
    }

    // Formatear columnas de Detalles de las Alertas
    private String limpiarNombreColumna(String nombre) {
        if (nombre == null)
            return "";
        String limpio = nombre.replace("VAL_", "");
        int ultimoUnderscore = limpio.lastIndexOf("_");
        if (ultimoUnderscore != -1) {
            limpio = limpio.substring(0, ultimoUnderscore);
        }
        return limpio.replace("_", " ").trim();
    }

    // Obtener los campos ALERTA
    private List<String> obtenerColumnasAlerta(String tabla) {
        String sql = """
                    SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = ?
                    AND COLUMN_NAME LIKE 'ALERTA_%'
                """;
        return jdbcTemplate.queryForList(sql, String.class, tabla);
    }

    // Obtener los Mensajes y Referencias para las ALERTAS
    private Map<String, Map<String, String>> obtenerTablaRef() {
        String sql = """
                    SELECT alert, message, column_ref
                    FROM cuipo_dev.dbo.RULES_ALERTS
                """;

        List<Map<String, Object>> datos = jdbcTemplate.queryForList(sql);

        Map<String, Map<String, String>> mapa = new HashMap<>();
        for (Map<String, Object> fila : datos) {
            Object alertObj = fila.get("alert");
            Object messageObj = fila.get("message");
            Object columnRefObj = fila.get("column_ref");

            if (alertObj == null || messageObj == null || columnRefObj == null) {
                continue;
            }

            String alert = alertObj.toString().trim();
            String message = messageObj.toString().trim();
            String columnRef = columnRefObj.toString().trim();

            Map<String, String> valores = new HashMap<>();
            valores.put("message", message);
            valores.put("column_ref", columnRef);

            mapa.put(alert, valores);
        }

        return mapa;
    }

}
