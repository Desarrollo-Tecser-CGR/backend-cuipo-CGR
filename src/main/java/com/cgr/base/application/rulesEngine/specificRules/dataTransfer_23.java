package com.cgr.base.application.rulesEngine.specificRules;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class dataTransfer_23 {

        @PersistenceContext
        private EntityManager entityManager;

        @Value("${TABLA_SPECIFIC_RULES}")
        private String tablaReglasEspecificas;

        @Value("${TABLA_EJEC_INGRESOS}")
        private String tablaEjecIngresos;

        @Transactional
        public void applySpecificRule23() {

                insertDataCA0086();
                insertDataCA0087();
                insertDataCA0088();
                insertDataCA0089();
                insertDataCA0090();
                insertDataCA0091();
                insertDataCA0092();

        }

        private void insertDataCA0086() {
                String createColumnCA0086 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'REGLA_ESPECIFICA_23A'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD REGLA_ESPECIFICA_23A VARCHAR(255); " +
                                "END; ";
                entityManager.createNativeQuery(createColumnCA0086).executeUpdate();
                String queryCA0086 = "WITH ICLD_Concepts AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    CUENTA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    NOM_FUENTES_FINANCIACION = 'INGRESOS CORRIENTES DE LIBRE DESTINACION' " +
                                "), " +
                                "ICLD_Desagregations AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    CUENTA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    NOM_FUENTES_FINANCIACION = 'ICLD LEY 99 - DESTINO AMBIENTAL' " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET REGLA_ESPECIFICA_23A = CASE " +
                                "    WHEN D.CUENTA IS NULL THEN 'NO CUMPLE' " +
                                "    ELSE 'CUMPLE' " +
                                "END " +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "JOIN ICLD_Concepts I ON SRD.CODIGO_ENTIDAD = I.CODIGO_ENTIDAD " +
                                "                     AND SRD.AMBITO_CODIGO = I.AMBITO_CODIGO " +
                                "LEFT JOIN ICLD_Desagregations D ON I.CODIGO_ENTIDAD = D.CODIGO_ENTIDAD " +
                                "                                 AND I.AMBITO_CODIGO = D.AMBITO_CODIGO " +
                                "                                 AND I.CUENTA = D.CUENTA " +
                                "WHERE SRD.CODIGO_ENTIDAD = I.CODIGO_ENTIDAD " +
                                "AND SRD.AMBITO_CODIGO = I.AMBITO_CODIGO;";
                entityManager.createNativeQuery(queryCA0086).executeUpdate();

        }

        private void insertDataCA0087() {
                String createColumnCA0087 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'ALERTA_CA0087'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD ALERTA_CA0087 VARCHAR(255); " +
                                "END; ";
                entityManager.createNativeQuery(createColumnCA0087).executeUpdate();
                String queryCA0087 = "WITH ICLD_Concepts AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    CUENTA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    NOM_FUENTES_FINANCIACION = 'INGRESOS CORRIENTES DE LIBRE DESTINACION' " +
                                "), " +
                                "ICLD_Desagregations AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    CUENTA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    NOM_FUENTES_FINANCIACION = 'ICLD LEY 99 - DESTINO AMBIENTAL' " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET ALERTA_CA0087 = CASE " +
                                "    WHEN D.CUENTA IS NULL THEN 'Alerta: Falta desagregación de ICLD – Ley 99 – Destino Ambiental' "
                                +
                                "    ELSE 'Desagregación encontrada' " +
                                "END " +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "JOIN ICLD_Concepts I ON SRD.CODIGO_ENTIDAD = I.CODIGO_ENTIDAD " +
                                "                     AND SRD.AMBITO_CODIGO = I.AMBITO_CODIGO " +
                                "LEFT JOIN ICLD_Desagregations D ON I.CODIGO_ENTIDAD = D.CODIGO_ENTIDAD " +
                                "                                 AND I.AMBITO_CODIGO = D.AMBITO_CODIGO " +
                                "                                 AND I.CUENTA = D.CUENTA " +
                                "WHERE SRD.CODIGO_ENTIDAD = I.CODIGO_ENTIDAD " +
                                "AND SRD.AMBITO_CODIGO = I.AMBITO_CODIGO;";

                entityManager.createNativeQuery(queryCA0087).executeUpdate();
        }

        private void insertDataCA0088() {
                String createColumnCA0088 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'ALERTA_CA0088'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD ALERTA_CA0088 VARCHAR(255); " +
                                "END; ";

                entityManager.createNativeQuery(createColumnCA0088).executeUpdate();
                String queryCA0088 = "WITH ICLD_Desagregations AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    NOM_TIPO_NORMA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    NOM_FUENTES_FINANCIACION = 'ICLD LEY 99 - DESTINO AMBIENTAL' " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET ALERTA_CA0088 = CASE " +
                                "    WHEN D.NOM_TIPO_NORMA <> 'LEY' OR D.NOM_TIPO_NORMA IS NULL THEN 'Alerta de calidad: Tipo de norma no es LEY' "
                                +
                                "    ELSE NULL " +
                                "END " +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "JOIN ICLD_Desagregations D ON SRD.CODIGO_ENTIDAD = D.CODIGO_ENTIDAD " +
                                "                           AND SRD.AMBITO_CODIGO = D.AMBITO_CODIGO;";

                entityManager.createNativeQuery(queryCA0088).executeUpdate();
        }

        private void insertDataCA0089() {
                String createColumnCA0089 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'REGLA_ESPECIFICA_23B'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD REGLA_ESPECIFICA_23B VARCHAR(255); " +
                                "END; ";

                entityManager.createNativeQuery(createColumnCA0089).executeUpdate();
                String queryCA0089 = "WITH ValidAccounts AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    CUENTA " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    CUENTA IN ( " +
                                "        '1.1.01.01.014', " +
                                "        '1.1.01.01.014.01', " +
                                "        '1.1.01.01.014.02', " +
                                "        '1.1.02.06.003.01.14' " +
                                "    ) " +
                                "    AND AMBITO_CODIGO IN ('A439', 'A438', 'A440') " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET REGLA_ESPECIFICA_23B = CASE " +
                                "    WHEN V.AMBITO_CODIGO NOT IN ('A439', 'A438', 'A440') THEN 'NO APLICA' " +
                                "    WHEN V.CUENTA IS NULL THEN 'NO CUMPLE' " +
                                "    ELSE 'CUMPLE' " +
                                "END " +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "LEFT JOIN ValidAccounts V ON SRD.CODIGO_ENTIDAD = V.CODIGO_ENTIDAD " +
                                "                          AND SRD.AMBITO_CODIGO = V.AMBITO_CODIGO;";

                entityManager.createNativeQuery(queryCA0089).executeUpdate();
        }

        private void insertDataCA0090() {
                String createColumnCA0090 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'ALERTA_CA0090'" +
                                "AND TABLE_SCHEMA = 'dbo'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD ALERTA_CA0090 VARCHAR(255); " +
                                "END; ";

                entityManager.createNativeQuery(createColumnCA0090).executeUpdate();
                String queryCA0090 = "WITH AccountsCheck AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    PERIODO, " +
                                "    MAX(CASE WHEN CUENTA IN ('1.1.01.01.014') THEN 1 ELSE 0 END) AS HasSobretasa, " +
                                "    MAX(CASE WHEN CUENTA = '1.1.02.06.003.01.14' THEN 1 ELSE 0 END) AS HasParticipacion "
                                +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    CUENTA IN ('1.1.01.01.014', '1.1.01.01.014.01', '1.1.01.01.014.02', '1.1.02.06.003.01.14') "
                                +
                                "    AND AMBITO_CODIGO IN ('A439', 'A438', 'A440') " +
                                "GROUP BY" +
                                "    CODIGO_ENTIDAD,  AMBITO_CODIGO, PERIODO) " +

                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET ALERTA_CA0090 = 'Alerta: No puede presentar ambos registros (sobretasa ambiental y participacion ambiental)'"
                                +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "INNER JOIN AccountsCheck AC" +
                                "    ON SRD.CODIGO_ENTIDAD = AC.CODIGO_ENTIDAD" +
                                "    AND SRD.AMBITO_CODIGO = AC.AMBITO_CODIGO" +
                                "    AND SRD.FECHA = AC.PERIODO" +
                                " WHERE AC.HasSobretasa = 1 AND AC.HasParticipacion = 1; ";

                entityManager.createNativeQuery(queryCA0090).executeUpdate();
        }

        private void insertDataCA0091() {
                String createColumnCA0091 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'REGLA_ESPECIFICA_23C'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD REGLA_ESPECIFICA_23C VARCHAR(255); " +
                                "END; ";
                entityManager.createNativeQuery(createColumnCA0091).executeUpdate();
                String queryCA0091 = "WITH SanAndresConcepts AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    PERIODO, " +
                                "    COUNT(DISTINCT CUENTA) AS ConceptCount " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    AMBITO_CODIGO = 'A441' " +
                                "    AND COD_FUENTES_FINANCIACION = '1.2.3.4.03' " +
                                "GROUP BY " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    PERIODO " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET REGLA_ESPECIFICA_23C = 'Alerta: No hay registro para cada concepto susceptible al cálculo del ICLD con fuente SAN ANDRES CON DESTINO A PROVIDENCIA.' "
                                +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "INNER JOIN SanAndresConcepts SAC " +
                                "    ON SRD.CODIGO_ENTIDAD = SAC.CODIGO_ENTIDAD " +
                                "    AND SRD.AMBITO_CODIGO = SAC.AMBITO_CODIGO " +
                                "    AND SRD.FECHA = SAC.PERIODO " +
                                "WHERE SAC.ConceptCount = 0;";

                entityManager.createNativeQuery(queryCA0091).executeUpdate();
        }

        private void insertDataCA0092() {
                String createColumnCA0092 = "IF NOT EXISTS (" +
                                "SELECT * " +
                                "FROM INFORMATION_SCHEMA.COLUMNS " +
                                "WHERE TABLE_NAME = 'SPECIFIC_RULES_DATA' " +
                                "AND COLUMN_NAME = 'ALERTA_CA0092'" +
                                ") " +
                                "BEGIN " +
                                "ALTER TABLE [cuipo_dev].[dbo].[SPECIFIC_RULES_DATA] " +
                                "ADD ALERTA_CA0092 VARCHAR(255); " +
                                "END; ";

                entityManager.createNativeQuery(createColumnCA0092).executeUpdate();
                String queryCA0092 = "WITH MissingConcepts AS ( " +
                                "SELECT " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    PERIODO " +
                                "FROM " +
                                "    VW_OPENDATA_B_EJECUCION_INGRESOS " +
                                "WHERE " +
                                "    AMBITO_CODIGO = 'A441' " +
                                "    AND COD_FUENTES_FINANCIACION = '1.2.3.4.03' " +
                                "GROUP BY " +
                                "    CODIGO_ENTIDAD, " +
                                "    AMBITO_CODIGO, " +
                                "    PERIODO " +
                                ") " +
                                "UPDATE SPECIFIC_RULES_DATA " +
                                "SET ALERTA_CA0092 = 'Alerta: No se registró concepto de desagregación con fuente SAN ANDRES CON DESTINO A PROVIDENCIA.' "
                                +
                                "FROM SPECIFIC_RULES_DATA SRD " +
                                "LEFT JOIN MissingConcepts MC " +
                                "    ON SRD.CODIGO_ENTIDAD = MC.CODIGO_ENTIDAD " +
                                "    AND SRD.AMBITO_CODIGO = MC.AMBITO_CODIGO " +
                                "    AND SRD.FECHA = MC.PERIODO " +
                                "WHERE MC.CODIGO_ENTIDAD IS NULL " +
                                "    AND SRD.AMBITO_CODIGO = 'A441';";
                entityManager.createNativeQuery(queryCA0092).executeUpdate();
        }

}
