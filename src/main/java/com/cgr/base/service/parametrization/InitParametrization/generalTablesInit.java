package com.cgr.base.service.parametrization.InitParametrization;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.repository.parametrization.generalRulesRepo;

import org.springframework.jdbc.core.JdbcTemplate;
import jakarta.transaction.Transactional;

@Service
public class generalTablesInit {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    generalRulesRepo GeneralRepo;

    public void initGeneralTables() {
        tableGeneralRulesName();
        tableGeneralRulesTable();
    }

    @Transactional
    public void tableGeneralRulesTable() {

        String checkTableQuery = """
                    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'GENERAL_RULES_NAMES')
                        SELECT 1 ELSE SELECT 0;
                """;

        Integer tableExists = jdbcTemplate.queryForObject(checkTableQuery, Integer.class);

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE GENERAL_RULES_NAMES (
                        CODIGO_REGLA NVARCHAR(25) NOT NULL,
                        NOMBRE_REGLA NVARCHAR(MAX) NOT NULL,
                        DESCRIPCION_REGLA NVARCHAR(MAX) NOT NULL,
                        ORDEN INT NOT NULL,
                        DETALLES INT NOT NULL,
                        REFERENCIA NVARCHAR(MAX) NOT NULL,
                        CODIGO_TABLA NVARCHAR(15) NOT NULL
                    );
                    """;
            jdbcTemplate.execute(createTableSQL);

            String insertDataSQL = """
                    MERGE INTO GENERAL_RULES_NAMES AS target
                    USING (VALUES
                        ('REGLA_GENERAL_1', 'Presupuesto Definitivo Mayor o Igual a $100M (NO Liquidación)', 'Validar que el Presupuesto Definitivo para la Cuenta 1:Ingresos NO sea Inferior a $100M, si el Nombre de la Entidad NO incluye "en Liquidación".', 1, 0, '0', '0'),
                        ('REGLA_GENERAL_2', 'Presupuesto Inicial y Presupuesto Definitivo diferentes de $0', 'Validar para todas las Cuentas que el Presupuesto Definitivo y el Presupuesto Inicial NO sean simultáneamente $0.', 2, 1, 'PRESUPUESTO_INICIAL, PRESUPUESTO_DEFINITIVO', 'PI'),
                        ('REGLA_GENERAL_3', 'Presupuesto Inicial Igual en Todos los Trimestres', 'Validar para todas las Cuentas que el Presupuesto Inicial en los Trimestres 2, 3 y 4 es Igual al Presupuesto Inicial en el Primer Trimestre.', 3, 1, 'PRESUPUESTO_INICIAL_1ER_TRIM, PRESUPUESTO_INICIAL', 'PI'),
                        ('REGLA_GENERAL_4A', 'Presupuesto  Inicial igual a Apropiación Inicial', 'Para las Entidades Territoriales, Validar que para la Cuenta 1:Ingresos que el Presupuesto Inicial  es igual a la sumatoria de la Apropiación Inicial para la Cuenta 2:Gastos (con Vigencia 1 y 4).', 4, 0, '0', '0'),
                        ('REGLA_GENERAL_4B', 'Presupuesto  Definitivo igual a Apropiación Definitiva', 'Para las Entidades Territoriales, Validar que para la Cuenta 1:Ingresos que el Presupuesto Definitivo es igual a la sumatoria de la Apropiación Definitiva para la Cuenta 2:Gastos (con Vigencia 1 y 4).', 5, 0, '0', '0'),
                        ('REGLA_GENERAL_5', 'Crecimiento del Recaudo Total por Trimestres', 'Validar para Todas las Cuentas de hasta Tercer Nivel, que el Recaudo Total en los Trimestres 2, 3 y 4, es Mayor al Recaudo Total del Trimestre Previo.', 6, 1, 'TOTAL_RECAUDO, TOTAL_RECAUDO_TRIMESTRE_PREVIO, DIFERENCIA_RECAUDO_TOTAL', 'EI'),
                        ('REGLA_GENERAL_6', 'Registro de Cuentas "1.0", "1.1" y "1.2" en Programacion y Ejecución de Ingresos', 'Validar para las Cuentas: "1.0", "1.1"  y "1.2", que si Existen Reportes en Ejecución de Gastos, tambien Existe el Reporte Equivalente en Programación de Gastos.  NO se valida la cuenta "1.0" para AMBITOS 438,439,440,441,442 y 452.', 7, 0, '0', '0'),
                        ('REGLA_GENERAL_7', 'Código Sección Presupuestal Acorde al Ámbito', 'Validar para Todas las  Cuentas que la Sección Presupuestal Corresponda a Alguno de los Definidos para el Ámbito.  Para las Entidades Territoriales(438 al 441), la Sección Presupuestal Corresponde del16 al 45. Para las Entidades Descentralizadas(442 al 454), la Sección Presupuestal Corresponde del 1 al 15.', 8, 1, 'COD_SECCION_PRESUPUESTAL, NOM_SECCION_PRESUPUESTAL', 'PG'),
                        ('REGLA_GENERAL_8', 'Vigencia del Gasto Acorde al Ámbito', 'Validar para Todas las  Cuentas que la Vigencia del Gasto, Corresponda a Alguno de los Establecidos de Acuerdo al Ámbito en el Formulario AMBITOS_CAPTURA.', 9, 1, 'COD_VIGENCIA_DEL_GASTO, NOM_VIGENCIA_DEL_GASTO', 'PG'),
                        ('REGLA_GENERAL_9A', 'Inexistencia de Cuentas "2.3" en Programación Gastos', 'Validar que NO se Reporten Cuentas que Inicien con "2.3" en Programación de Gastos.', 10, 1, '0', 'PG'),
                        ('REGLA_GENERAL_9B', 'Existencia de Cuenta "2.99" en Programación Gastos', 'Validar en Programación de Gastos que Existen Reportes con Cuenta "2.99".', 11, 0, '0', '0'),
                        ('REGLA_GENERAL_10A', 'Apropiación Definitiva Mayor o Igual a $100M  para Cuenta "2"', 'Validar para la Cuenta 2:Gastos que la Sumatoria de la Apropiación Definitiva es Mayor a $100M,  para las Vigencias del Gasto que le Correspondan segun el Ámbito en AMBITOS_CAPTURA.', 12, 0, '0', '0'),
                        ('REGLA_GENERAL_10B', 'Apropiación Definitiva Diferente de $0 para Cuentas de 2do Nivel', 'Validar para las Cuentas: "2.1", "2.2"  y "2.4", que   la Apropiación Definitiva es diferente de  $0.', 13, 0, '0', '0'),
                        ('REGLA_GENERAL_10C', 'Apropiación Definitiva Mayor o Igual a $100M para Cuenta "2.99"', 'Validar  para la Cuenta 2.99  que la Sumatoria de la Apropiación Definitiva es Mayor a $100M.', 14, 0, '0', '0'),
                        ('REGLA_GENERAL_11', 'Validación Apropiación Inicial  Respecto al Primer Trimestre', 'Validar para todas las Cuentas que la sumatoria de la Apropiación Inicial (con Vigencia 1 y 4) en los Trimestres 2, 3 y 4 es Igual a la sumatoria de la Apropiación Inicial (con Vigencia 1 y 4) en el Trimestre 1.', 15, 1, 'APROP_INICIAL_TRIM_VAL, APROP_INICIAL_1ER_TRIM', 'PG'),
                        ('REGLA_GENERAL_12A', 'Compromisos Mayor o Igual a Obligaciones', 'Validar para todas las Cuentas que los Compromisos son  Igual o Mayor que las Obligaciones(Entidades que reportan Compromisos). Calcular la Razon Porcentual.', 16, 1, 'COMPROMISOS, OBLIGACIONES, RAZON_COMPROMISO_OBLIGACIONES', 'EG'),
                        ('REGLA_GENERAL_12B', 'Obligaciones Mayor o Igual a Pagos', 'Validar para todas las Cuentas que las Obligaciones son  Igual o Mayor que los Pagos. Calcular la Razon Porcentual.', 17, 1, 'OBLIGACIONES, PAGOS, RAZON_PAGO_OBLIGACIONES', 'EG'),
                        ('REGLA_GENERAL_13A', 'Registro de Cuentas "2,1", "2,2" y "2.4" en Programacion y Ejecución de Gastos', 'Validar para las Cuentas: "2.1", "2.2"  y "2.4", que si Existen Reportes en Ejecución de Gastos, tambien Existe el Reporte Equivalente en Programación de Gastos.', 18, 0, '0', '0'),
                        ('REGLA_GENERAL_13B', 'Registro de Cuentas "2,3" en Programación de Gastos y  "2.99" en Ejecución de Gastos', 'Validar para la Cuenta: "2.3" en Ejecución de Gastos, que  Existe el Reporte con Cuenta "2.99" en Programación de Gastos.', 19, 0, '0', '0'),
                        ('REGLA_GENERAL_13C', 'Inexistencia Cuenta "2.99" en Ejecución de Gastos', 'Validar en Ejecución de Gastos que NO Existen Reportes con Cuenta "2.99".', 20, 0, '0', '0'),
                        ('REGLA_GENERAL_14A', 'Existencia de Vigencia Actual en Programación y Ejecución de Gastos', 'Validar en los Formularios Ejecución de Gastos y Programación de Gastos, que Exista Algun Registro con Vigencia del Gasto 1:Vigencia Actual.', 21, 0, '0', '0'),
                        ('REGLA_GENERAL_14B', 'Vigencia del Gasto Igual en Programación y Ejecución de Gastos', 'Validar en los Formularios Ejecución de Gastos y Programación de Gastos, que Reporten las Mismas  Vigencias de Gasto.', 22, 0, '0', '0'),
                        ('REGLA_GENERAL_15', 'Consistencia Vigencias de Programación y Ejecución de Gastos', 'Validar que la Vigencia del Gasto en Programación de Gastos sea Igual a la Vigencia del Gasto en Ejecución de Gastos.', 23, 1, 'ALERTA_15, REGLA_GENERAL_15', 'PG'),
                        ('REGLA_GENERAL_15', 'Consistencia entr Código CPC y Concepto Presupuestal', 'Validar para Todas las Cuentas  Habilitadas para el Uso de la Variable CPC, que el Úlltimo Dígito de la Cuenta Coincide con el Primer Dígito del Código CPC.', 23, 1, 'COD_CPC, NOM_CPC', 'EG'),
                        ('REGLA_GENERAL_16', 'Datos Recaudo Presupuestal de Ingresos Consistentes', 'Validar que el Recaudo Total (Columna D) sea igual a la suma de las columnas E,F,G y H del Recaudo.', 24, 1, 'COL_D_RECAUDO, SUM_COLS_EFGH_RECAUDO', 'EI'),
                        ('REGLA_GENERAL_16A', 'Validación de Variaciones Extremas Trimestrales en Ejecución de Gastos', 'Validar para la Cuenta 2:Gastos que  las Obligaciones(Entidades que NO reportan Compromisos) o los Compromisos(Entidades que reportan Compromisos),  NO Presentan Variaciones Negativas Superiores al 20% o Variaciones Positivas Superiores al 30% en los Trimestres 2, 3 y 4  Respecto al Trimestre Previo.', 24, 0, '0', '0'),
                        ('REGLA_GENERAL_16B', 'Validación de Variaciones Extremas Anuales en Ejecución de Gastos', 'Validar para todas las Cuentas que las Obligaciones(Entidades que NO reportan Compromisos) o los Compromisos(Entidades que reportan Compromisos),  NO Presentan Variaciones Negativas Superiores al 20% o Variaciones Positivas Superiores al 30% en cada Trimestre Respecto al mismo Trimestre del Año Previo.', 25, 1, 'COMP_OBLIG_PERIODO_VAL, COMP_OBLIG_PERIODO_PREV, VAR_PORCENTUAL_COMP_OBLIG, VAR_MONETARIA_COMP_OBLIG', 'EG'),
                        ('REGLA_GENERAL_17A', 'Validación de Variaciones Extremas Trimestrales en Ejecución de Ingresos', 'Validar para la Cuenta 2:Gastos que  las Obligaciones(Entidades que NO reportan Compromisos) o los Compromisos(Entidades que reportan Compromisos),  NO Presentan Variaciones Negativas Superiores al 20% o Variaciones Positivas Superiores al 30% en los Trimestres 2, 3 y 4  Respecto al Trimestre Previo.', 26, 0, '0', '0'),
                        ('REGLA_GENERAL_17B', 'Validación de Variaciones Extremas Anuales en Ejecución de Ingresos', 'Validar para todas las Cuentas que el Recaudo Total,  NO Presente Variaciones Negativas Superiores al 20% o Variaciones Positivas Superiores al 30% en cada Trimestre Respecto al mismo Trimestre del Año Previo.', 27, 1, 'RECAUDO_TOTAL_PERIODO_VAL, RECAUDO_TOTAL_PERIODO_PREV, VAR_PORCENTUAL_RECAUDO_TOTAL, VAR_MONETARIA_RECAUDO_TOTAL', 'EI')
                    ) AS source(CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA, ORDEN, DETALLES, REFERENCIA, CODIGO_TABLA)
                    ON target.CODIGO_REGLA = source.CODIGO_REGLA
                    WHEN NOT MATCHED THEN
                      INSERT (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA, ORDEN, DETALLES, REFERENCIA, CODIGO_TABLA)
                      VALUES (source.CODIGO_REGLA, source.NOMBRE_REGLA, source.DESCRIPCION_REGLA, source.ORDEN, source.DETALLES, source.REFERENCIA, source.CODIGO_TABLA);
                    """;

            jdbcTemplate.execute(insertDataSQL);
        }
    }

    @Transactional
    public void tableGeneralRulesName() {

        String checkTableQuery2 = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'GENERAL_RULES_TABLES')
                SELECT 1 ELSE SELECT 0;
                """;
        Integer tableExists = jdbcTemplate.queryForObject(checkTableQuery2, Integer.class);

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE GENERAL_RULES_TABLES (
                        CODIGO_TABLA NVARCHAR(10) NOT NULL,
                        NOMBRE_TABLA NVARCHAR(255) NOT NULL,
                        CODIGO_REGLA NVARCHAR(10) NOT NULL,
                        NOMBRE_REGLA NVARCHAR(50) NOT NULL
                    );
                    """;
            jdbcTemplate.execute(createTableSQL);

            String insertDataSQL = """
                    MERGE INTO GENERAL_RULES_TABLES AS target
                    USING (VALUES
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '1', 'REGLA_GENERAL_1'),
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '2', 'REGLA_GENERAL_2'),
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '3', 'REGLA_GENERAL_3'),
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '4A', 'REGLA_GENERAL_4A'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '4A', 'REGLA_GENERAL_4A'),
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '4B', 'REGLA_GENERAL_4B'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '4B', 'REGLA_GENERAL_4B'),
                        ('EI', 'VW_OPENDATA_B_EJECUCION_INGRESOS', '5', 'REGLA_GENERAL_5'),
                        ('PI', 'VW_OPENDATA_A_PROGRAMACION_INGRESOS', '6', 'REGLA_GENERAL_6'),
                        ('EI', 'VW_OPENDATA_B_EJECUCION_INGRESOS', '6', 'REGLA_GENERAL_6'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '7', 'REGLA_GENERAL_7'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '8', 'REGLA_GENERAL_8'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '9A', 'REGLA_GENERAL_9A'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '9B', 'REGLA_GENERAL_9B'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '10A', 'REGLA_GENERAL_10A'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '10B', 'REGLA_GENERAL_10B'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '10C', 'REGLA_GENERAL_10C'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '11', 'REGLA_GENERAL_11'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '12A', 'REGLA_GENERAL_12A'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '12B', 'REGLA_GENERAL_12B'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '13A', 'REGLA_GENERAL_13A'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '13A', 'REGLA_GENERAL_13A'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '13B', 'REGLA_GENERAL_13B'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '13B', 'REGLA_GENERAL_13B'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '13C', 'REGLA_GENERAL_13C'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '14A', 'REGLA_GENERAL_14A'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '14A', 'REGLA_GENERAL_14A'),
                        ('PG', 'VW_OPENDATA_C_PROGRAMACION_GASTOS', '14B', 'REGLA_GENERAL_14B'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '14B', 'REGLA_GENERAL_14B'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '15', 'REGLA_GENERAL_15'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '16A', 'REGLA_GENERAL_16A'),
                        ('EG', 'VW_OPENDATA_D_EJECUCION_GASTOS', '16B', 'REGLA_GENERAL_16B'),
                        ('EI', 'VW_OPENDATA_B_EJECUCION_INGRESOS', '17A', 'REGLA_GENERAL_17A'),
                        ('EI', 'VW_OPENDATA_B_EJECUCION_INGRESOS', '17B', 'REGLA_GENERAL_17B')
                    ) AS source (CODIGO_TABLA, NOMBRE_TABLA, CODIGO_REGLA, NOMBRE_REGLA)

                    ON target.CODIGO_TABLA = source.CODIGO_TABLA
                    AND target.NOMBRE_TABLA = source.NOMBRE_TABLA
                    AND target.CODIGO_REGLA = source.CODIGO_REGLA

                    WHEN NOT MATCHED THEN
                    INSERT (CODIGO_TABLA, NOMBRE_TABLA, CODIGO_REGLA, NOMBRE_REGLA)
                    VALUES (source.CODIGO_TABLA, source.NOMBRE_TABLA, source.CODIGO_REGLA, source.NOMBRE_REGLA);
                    """;
            jdbcTemplate.execute(insertDataSQL);
        }
    }

}
