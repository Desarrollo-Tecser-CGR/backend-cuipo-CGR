package com.cgr.base.application.rulesEngine.specificRules;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service

public class dataTransfer_29 {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${TABLA_EJEC_GASTOS}")
    private String ejecGastos;

    @Value("${TABLA_E029}")
    private String tablaE029;

    @PersistenceContext
    private EntityManager entityManager;

    @Transactional
    public void applySpecificRule29A() {

        String sqlCheckTable = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'E029'";
        String sqlCreateTable = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TABLA_E029')"
                +
                " BEGIN " +
                " CREATE TABLE [" + tablaE029 + "] (" +
                "FECHA varchar(50)," +
                "TRIMESTRE varchar(50)," +
                "CODIGO_ENTIDAD VARCHAR(50), " +
                "AMBITO_CODIGO VARCHAR(50), " +
                "GASTOS_FUNCIONAMIENTO_ASAM DECIMAL(18,2), " +
                "CATEGORIA VARCHAR(50), " +
                "NO_DIPUTADOS INT, " +
                "LIM_GASTO_ASAMBLEA DECIMAL(18,2), " +
                "MAX_SESIONES_ASAM INT, " +
                "REMU_DIPUTADOS_SMMLV DECIMAL(18,2), " +
                "SMMLV DECIMAL(18,2), " +
                "GASTOS_ASAMBLEA DECIMAL(18,2), " +
                "REMUNERACION_DIPUTADOS DECIMAL(18,2), " +
                "PRESTACIONES_SOCIALES DECIMAL(18,2), " +
                "ALERTA VARCHAR(200), " +
                "CUENTAS VARCHAR(MAX)" +
                "); " +
                "END";
        Integer count = (Integer) entityManager.createNativeQuery(sqlCheckTable).getSingleResult();

        if (count == 0) {
            entityManager.createNativeQuery(sqlCreateTable).executeUpdate();
        }

        List<String> requiredColumns = Arrays.asList(
                "FECHA",
                "TRIMESTRE",
                "CODIGO_ENTIDAD",
                "AMBITO_CODIGO",
                "GASTOS_FUNCIONAMIENTO_ASAM",
                "CATEGORIA",
                "NO_DIPUTADOS",
                "LIM_GASTO_ASAMBLEA",
                "MAX_SESIONES_ASAM",
                "REMU_DIPUTADOS_SMMLV",
                "SMMLV",
                "GASTOS_ASAMBLEA",
                "REMUNERACION_DIPUTADOS",
                "PRESTACIONES_SOCIALES",
                "ALERTA_29A",
                "CUENTAS");

        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaE029,
                "'" + String.join("','", requiredColumns) + "'");
        List<String> existingCols = jdbcTemplate.queryForList(checkColumnsQuery, String.class);
        for (String col : requiredColumns) {
            if (!existingCols.contains(col)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s VARCHAR(MAX) NULL",
                        tablaE029, col);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // 2. Construir la consulta INSERT utilizando WITH.
        // Se inyecta la tabla de ejecución de gastos (ejecGastos) en todas las
        // referencias al origen
        // y se utiliza tablaE029 para la tabla destino.
        String insertQuery = String.format("""
                ;WITH
                Main AS (
                    SELECT
                        g.FECHA,
                        g.TRIMESTRE,
                        g.CODIGO_ENTIDAD,
                        g.AMBITO_CODIGO,
                        SUM(CAST(g.COMPROMISOS AS FLOAT)) AS GASTOS_FUNCIONAMIENTO_ASAM,
                        c.CATEGORIA,
                        c.NO_DIPUTADOS,
                        p.LIM_GASTO_ASAMBLEA,
                        p.MAX_SESIONES_ASAM,
                        p.REMU_DIPUTADOS_SMMLV,
                        pa.SMMLV
                    FROM %s AS g
                    LEFT JOIN cuipo_dev.dbo.CATEGORIAS AS c
                        ON g.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD
                       AND g.AMBITO_CODIGO = c.AMBITO_CODIGO
                    LEFT JOIN cuipo_dev.dbo.PORCENTAJES_LIMITES AS p
                        ON p.AMBITO_CODIGO = c.AMBITO_CODIGO
                       AND p.CATEGORIA_CODIGO = c.CATEGORIA
                    LEFT JOIN cuipo_dev.dbo.PARAMETRIZACION_ANUAL AS pa
                        ON g.FECHA = pa.FECHA
                    WHERE g.TRIMESTRE = '12'
                      AND (g.AMBITO_CODIGO = 'A438' OR g.AMBITO_CODIGO = 'A441')
                    GROUP BY
                        g.FECHA,
                        g.TRIMESTRE,
                        g.CODIGO_ENTIDAD,
                        g.AMBITO_CODIGO,
                        c.CATEGORIA,
                        c.NO_DIPUTADOS,
                        p.REMU_DIPUTADOS_SMMLV,
                        p.LIM_GASTO_ASAMBLEA,
                        p.MAX_SESIONES_ASAM,
                        pa.SMMLV
                ),
                Asamblea AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        SUM(COMPROMISOS) AS GASTOS_ASAMBLEA
                    FROM %s
                    WHERE CUENTA = '2'
                      AND COD_SECCION_PRESUPUESTAL = '19'
                      AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                    GROUP BY
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO
                ),
                RemuDip AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        SUM(COMPROMISOS) AS REMUNERACION_DIPUTADOS
                    FROM %s
                    WHERE CUENTA = '2.1.1.01.01.001.11'
                      AND COD_SECCION_PRESUPUESTAL = '19'
                      AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                    GROUP BY
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO
                ),
                CuentasEsperadas AS (
                    SELECT '2.1.1.01.01.001.08.03' AS CUENTA UNION ALL
                    SELECT '2.1.1.01.01.001.08.04' UNION ALL
                    SELECT '2.1.1.01.03.001.05'     UNION ALL
                    SELECT '2.1.1.01.02.020.01'     UNION ALL
                    SELECT '2.1.1.01.02.020.02'     UNION ALL
                    SELECT '2.1.1.01.02.020.03'     UNION ALL
                    SELECT '2.1.1.01.02.020.04'     UNION ALL
                    SELECT '2.1.1.01.02.020.05'     UNION ALL
                    SELECT '2.1.1.01.02.020.06'     UNION ALL
                    SELECT '2.1.1.01.02.020.07'     UNION ALL
                    SELECT '2.1.1.01.02.020.08'     UNION ALL
                    SELECT '2.1.1.01.02.020.09'
                ),
                DatosPrestaciones AS (
                    SELECT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        SUM(COMPROMISOS) AS PRESTACIONES_SOCIALES
                    FROM %s
                    WHERE CUENTA IN (SELECT CUENTA FROM CuentasEsperadas)
                      AND COD_SECCION_PRESUPUESTAL = '19'
                      AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                    GROUP BY
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO
                ),
                CuentasReportadas AS (
                    SELECT DISTINCT
                        FECHA,
                        TRIMESTRE,
                        CODIGO_ENTIDAD,
                        AMBITO_CODIGO,
                        CUENTA
                    FROM %s
                    WHERE CUENTA IN (SELECT CUENTA FROM CuentasEsperadas)
                      AND COD_SECCION_PRESUPUESTAL = '19'
                      AND (COD_VIGENCIA_DEL_GASTO = '1' OR COD_VIGENCIA_DEL_GASTO = '4')
                ),
                MissingAccounts AS (
                    SELECT
                        d.FECHA,
                        d.TRIMESTRE,
                        d.CODIGO_ENTIDAD,
                        d.AMBITO_CODIGO,
                        STRING_AGG(ce.CUENTA, ', ') AS CUENTAS_FALTANTES
                    FROM DatosPrestaciones d
                    CROSS JOIN CuentasEsperadas ce
                    LEFT JOIN CuentasReportadas cr
                        ON d.FECHA = cr.FECHA
                       AND d.TRIMESTRE = cr.TRIMESTRE
                       AND d.CODIGO_ENTIDAD = cr.CODIGO_ENTIDAD
                       AND d.AMBITO_CODIGO = cr.AMBITO_CODIGO
                       AND ce.CUENTA = cr.CUENTA
                    WHERE cr.CUENTA IS NULL
                    GROUP BY
                        d.FECHA,
                        d.TRIMESTRE,
                        d.CODIGO_ENTIDAD,
                        d.AMBITO_CODIGO
                ),
                Prestaciones AS (
                    SELECT
                        d.FECHA,
                        d.TRIMESTRE,
                        d.CODIGO_ENTIDAD,
                        d.AMBITO_CODIGO,
                        d.PRESTACIONES_SOCIALES,
                        CASE
                            WHEN m.CUENTAS_FALTANTES IS NULL
                                THEN 'LA ENTIDAD SATISFACE LOS CRITERIOS DE VALIDACIÓN'
                            ELSE 'LA ENTIDAD NO REPORTÓ GASTOS DE SEGURIDAD SOCIAL PARA UNA DE LAS CUENTAS'
                        END AS ALERTA_29A,
                        ISNULL(m.CUENTAS_FALTANTES, '') AS CUENTAS
                    FROM DatosPrestaciones d
                    LEFT JOIN MissingAccounts m
                        ON d.FECHA = m.FECHA
                       AND d.TRIMESTRE = m.TRIMESTRE
                       AND d.CODIGO_ENTIDAD = m.CODIGO_ENTIDAD
                       AND d.AMBITO_CODIGO = m.AMBITO_CODIGO
                )
                INSERT INTO %s (
                    FECHA,
                    TRIMESTRE,
                    CODIGO_ENTIDAD,
                    AMBITO_CODIGO,
                    GASTOS_FUNCIONAMIENTO_ASAM,
                    CATEGORIA,
                    NO_DIPUTADOS,
                    LIM_GASTO_ASAMBLEA,
                    MAX_SESIONES_ASAM,
                    REMU_DIPUTADOS_SMMLV,
                    SMMLV,
                    GASTOS_ASAMBLEA,
                    REMUNERACION_DIPUTADOS,
                    PRESTACIONES_SOCIALES,
                    ALERTA_29A,
                    CUENTAS
                )
                SELECT
                    m.FECHA,
                    m.TRIMESTRE,
                    m.CODIGO_ENTIDAD,
                    m.AMBITO_CODIGO,
                    m.GASTOS_FUNCIONAMIENTO_ASAM,
                    m.CATEGORIA,
                    m.NO_DIPUTADOS,
                    m.LIM_GASTO_ASAMBLEA,
                    m.MAX_SESIONES_ASAM,
                    m.REMU_DIPUTADOS_SMMLV,
                    m.SMMLV,
                    a.GASTOS_ASAMBLEA,
                    r.REMUNERACION_DIPUTADOS,
                    p.PRESTACIONES_SOCIALES,
                    CASE
                        WHEN p.PRESTACIONES_SOCIALES IS NULL
                            THEN 'LA ENTIDAD NO REPORTÓ GASTOS DE SEGURIDAD SOCIAL'
                        ELSE p.ALERTA_29A
                    END AS ALERTA_29A,
                    p.CUENTAS
                FROM Main m
                LEFT JOIN Asamblea a
                    ON m.FECHA = a.FECHA
                   AND m.TRIMESTRE = a.TRIMESTRE
                   AND m.CODIGO_ENTIDAD = a.CODIGO_ENTIDAD
                   AND m.AMBITO_CODIGO = a.AMBITO_CODIGO
                LEFT JOIN RemuDip r
                    ON m.FECHA = r.FECHA
                   AND m.TRIMESTRE = r.TRIMESTRE
                   AND m.CODIGO_ENTIDAD = r.CODIGO_ENTIDAD
                   AND m.AMBITO_CODIGO = r.AMBITO_CODIGO
                LEFT JOIN Prestaciones p
                    ON m.FECHA = p.FECHA
                   AND m.TRIMESTRE = p.TRIMESTRE
                   AND m.CODIGO_ENTIDAD = p.CODIGO_ENTIDAD
                   AND m.AMBITO_CODIGO = p.AMBITO_CODIGO
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM %s t
                    WHERE t.FECHA = m.FECHA
                      AND t.TRIMESTRE = m.TRIMESTRE
                      AND t.CODIGO_ENTIDAD = m.CODIGO_ENTIDAD
                      AND t.AMBITO_CODIGO = m.AMBITO_CODIGO
                )
                """,
                // Reemplazos para ejecGastos en cada referencia del origen:
                ejecGastos, // Main
                ejecGastos, // Asamblea
                ejecGastos, // RemuDip
                ejecGastos, // DatosPrestaciones
                ejecGastos, // CuentasReportadas
                // Reemplazos para tabla destino:
                tablaE029, // INSERT INTO
                tablaE029 // WHERE NOT EXISTS
        );

        // 3. Ejecutar la consulta de inserción.
        jdbcTemplate.execute(insertQuery);
    }

    @Transactional
    public void applySpecificRule29B() {
        // Lista de columnas que se actualizarán
        List<String> requiredColumns = Arrays.asList(
                "IBC",
                "CESANTIAS",
                "APORTES_PARAFISCALES",
                "SALUD",
                "PENSION",
                "RIESGOS_PROFESIONALES",
                "INGRESOS_CESANTIAS",
                "VACACIONES",
                "PRIMA_VACACIONES",
                "PRIMA_NAVIDAD");

        // Verificar que las columnas existan en la tabla
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaE029,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery = String.format(
                        "ALTER TABLE %s ADD %s DECIMAL(18,2) NULL",
                        tablaE029, column);
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // Construir la consulta UPDATE usando una CTE para realizar los cálculos
        String query = "UPDATE E029 " +
                "SET PRESTACIONES_SOCIALES = " +
                "(REMUNERACION_DIPUTADOS * pa.APORTES_PARAFISCALES) + " +
                "(REMUNERACION_DIPUTADOS * pa.SALUD) + " +
                "(REMUNERACION_DIPUTADOS * pa.PENSION) + " +
                "(REMUNERACION_DIPUTADOS * pa.RIESGOS_PROFESIONALES) + " +
                "(GASTOS_FUNCIONAMIENTO_ASAM / pa.CESANTIAS) + " +
                "((GASTOS_FUNCIONAMIENTO_ASAM / pa.CESANTIAS) * pa.INTERESES_CESANTIAS) + " +
                "((GASTOS_FUNCIONAMIENTO_ASAM / 30) * pa.VACAIONES) + " +
                "((GASTOS_FUNCIONAMIENTO_ASAM / 30) * pa.PRIMA_VACACIONES) + " +
                "(GASTOS_FUNCIONAMIENTO_ASAM + (((GASTOS_FUNCIONAMIENTO_ASAM / 30) * pa.PRIMA_VACACIONES) * pa.PRIMA_NAVIDAD)) "
                +
                "FROM E029 e " +
                "JOIN PARAMETRIZACION_ANUAL pa " +
                "ON e.FECHA = pa.FECHA";
        jdbcTemplate.execute(query);
    }

    @Transactional
    public void applySpecificRule29C() {
        // Lista de columnas a actualizar
        List<String> requiredColumns = Arrays.asList(
                "PS_SS_VALOR_MAXIMO_AUTORIZADO",
                "CONTROL_PS_SS",
                "MAXIMO_AUTORIZADO_REMU_DIP",
                "CONTROL_REMU_DIP",
                "OTROS_GASTOS_ASAM",
                "RELACION_GASTOS_EJECUTADOS",
                "ALERTA_29C");

        // Verificar si las columnas existen en la tabla
        String checkColumnsQuery = String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME IN (%s)",
                tablaE029,
                "'" + String.join("','", requiredColumns) + "'");

        List<String> existingColumns = jdbcTemplate.queryForList(checkColumnsQuery, String.class);

        // Si no existen, se crean: se usan DECIMAL(18,2) para los campos numéricos y
        // VARCHAR(MAX) para ALERTA.
        for (String column : requiredColumns) {
            if (!existingColumns.contains(column)) {
                String addColumnQuery;
                if ("ALERTA_29C".equals(column)) {
                    addColumnQuery = String.format("ALTER TABLE %s ADD %s VARCHAR(MAX) NULL", tablaE029, column);
                } else {
                    addColumnQuery = String.format("ALTER TABLE %s ADD %s DECIMAL(18,2) NULL", tablaE029, column);
                }
                jdbcTemplate.execute(addColumnQuery);
            }
        }

        // Construir la consulta UPDATE utilizando una CTE para realizar los cálculos
        String updateQuery = String.format(
                """
                        WITH Calculos AS (
                            SELECT
                                FECHA,
                                TRIMESTRE,
                                CODIGO_ENTIDAD,
                                AMBITO_CODIGO,
                                (APORTES_PARAFISCALES + SALUD + PENSION + RIESGOS_PROFESIONALES + CESANTIAS + INGRESOS_CESANTIAS + VACACIONES + PRIMA_VACACIONES + PRIMA_NAVIDAD) AS PS_SS_VALOR_MAXIMO_AUTORIZADO,
                                (PRESTACIONES_SOCIALES / (APORTES_PARAFISCALES + SALUD + PENSION + RIESGOS_PROFESIONALES + CESANTIAS + INGRESOS_CESANTIAS + VACACIONES + PRIMA_VACACIONES + PRIMA_NAVIDAD)) AS CONTROL_PS_SS,
                                (NO_DIPUTADOS * MAX_SESIONES_ASAM * (REMU_DIPUTADOS_SMMLV * SMMLV)) AS MAXIMO_AUTORIZADO_REMU_DIP,
                                (REMUNERACION_DIPUTADOS / (NO_DIPUTADOS * MAX_SESIONES_ASAM * (REMU_DIPUTADOS_SMMLV * SMMLV))) AS CONTROL_REMU_DIP,
                                (GASTOS_ASAMBLEA - REMUNERACION_DIPUTADOS - PRESTACIONES_SOCIALES) AS OTROS_GASTOS_ASAM,
                                (((GASTOS_ASAMBLEA - REMUNERACION_DIPUTADOS - PRESTACIONES_SOCIALES) / REMUNERACION_DIPUTADOS) * 100) AS RELACION_GASTOS_EJECUTADOS,
                                CASE
                                    WHEN REMUNERACION_DIPUTADOS IS NULL
                                        OR LIM_GASTO_ASAMBLEA IS NULL

                                    THEN 'La entidad presente posibles inconsistencias'

                                    WHEN (((GASTOS_ASAMBLEA - REMUNERACION_DIPUTADOS - PRESTACIONES_SOCIALES)
                                        / REMUNERACION_DIPUTADOS) * 100) <= LIM_GASTO_ASAMBLEA
                                    THEN 'La entidad excede los límites'

                                    ELSE 'La entidad NO excede los límites'
                                END AS ALERTA_29C

                            FROM %s
                        )
                        UPDATE e
                        SET
                            e.PS_SS_VALOR_MAXIMO_AUTORIZADO = c.PS_SS_VALOR_MAXIMO_AUTORIZADO,
                            e.CONTROL_PS_SS = c.CONTROL_PS_SS,
                            e.MAXIMO_AUTORIZADO_REMU_DIP = c.MAXIMO_AUTORIZADO_REMU_DIP,
                            e.CONTROL_REMU_DIP = c.CONTROL_REMU_DIP,
                            e.OTROS_GASTOS_ASAM = c.OTROS_GASTOS_ASAM,
                            e.RELACION_GASTOS_EJECUTADOS = c.RELACION_GASTOS_EJECUTADOS,
                            e.ALERTA_29C = c.ALERTA_29C
                        FROM %s e
                        INNER JOIN Calculos c
                            ON e.FECHA = c.FECHA
                           AND e.TRIMESTRE = c.TRIMESTRE
                           AND e.CODIGO_ENTIDAD = c.CODIGO_ENTIDAD
                           AND e.AMBITO_CODIGO = c.AMBITO_CODIGO;
                        """,
                tablaE029, tablaE029);

        jdbcTemplate.execute(updateQuery);
    }

}
