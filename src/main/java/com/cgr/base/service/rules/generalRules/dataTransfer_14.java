package com.cgr.base.service.rules.generalRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import com.cgr.base.service.rules.utils.dataBaseUtils;

@Service
public class dataTransfer_14 {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private dataBaseUtils UtilsDB;

	@Value("${TABLA_EJEC_GASTOS}")
	private String TABLA_EJEC_GASTOS;

	@Value("${TABLA_PROG_GASTOS}")
	private String TABLA_PROG_GASTOS;

	public void applyGeneralRule14() {
		applyGeneralRule14A();
		applyGeneralRule14B();
	}

	public void applyGeneralRule14B() {
		UtilsDB.ensureColumnsExist(
				"GENERAL_RULES_DATA",
				"REGLA_GENERAL_14B:NVARCHAR(15)",
				"ALERTA_14B:NVARCHAR(20)",
				"VAL_Vigencias_EjecGastos_14B:NVARCHAR(100)",
				"VAL_Vigencias_ProgGastos_14B:NVARCHAR(100)");

		String updateVigenciasEjecGastos14B = String.format(
				"""
						UPDATE d
						SET VAL_Vigencias_EjecGastos_14B = sub.VIGENCIAS
						FROM GENERAL_RULES_DATA d
						OUTER APPLY (
						    SELECT STRING_AGG(CAST(V AS VARCHAR), ',') AS VIGENCIAS
						    FROM (
						        SELECT DISTINCT TRY_CAST(TRY_CAST(a.COD_VIGENCIA_DEL_GASTO AS FLOAT) AS INT) AS V
						        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						        WHERE a.FECHA = d.FECHA
						          AND a.TRIMESTRE = d.TRIMESTRE
						          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
						          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
						    ) AS vigencias
						) sub
						""",
				TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
		jdbcTemplate.execute(updateVigenciasEjecGastos14B);

		String updateVigenciasProgGastos14B = String.format(
				"""
						UPDATE d
						SET VAL_Vigencias_ProgGastos_14B = sub.VIGENCIAS
						FROM GENERAL_RULES_DATA d
						OUTER APPLY (
						    SELECT STRING_AGG(CAST(V AS VARCHAR), ',') AS VIGENCIAS
						    FROM (
						        SELECT DISTINCT TRY_CAST(TRY_CAST(a.COD_VIGENCIA_DEL_GASTO AS FLOAT) AS INT) AS V
						        FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						        WHERE a.FECHA = d.FECHA
						          AND a.TRIMESTRE = d.TRIMESTRE
						          AND a.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
						          AND a.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
						    ) AS vigencias
						) sub
						""",
				TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
		jdbcTemplate.execute(updateVigenciasProgGastos14B);

		String updateNoDataQuery = String.format(
				"""
						UPDATE GENERAL_RULES_DATA
						SET REGLA_GENERAL_14B = 'SIN DATOS',
						    ALERTA_14B = 'NO_PG'
						WHERE NOT EXISTS (
						    SELECT 1
						    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
						    AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
						    AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
						    AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
						)
						""", TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);

		jdbcTemplate.update(updateNoDataQuery);

		String updateNoDataQuery2 = String.format(
				"""
						UPDATE GENERAL_RULES_DATA
						SET REGLA_GENERAL_14B = 'SIN DATOS',
						    ALERTA_14B = 'NO_EG'
						WHERE NOT EXISTS (
						    SELECT 1
						    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
						    AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
						    AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
						    AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
						)
						""", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);

		jdbcTemplate.update(updateNoDataQuery2);

		String updateRegla14B = """
				UPDATE d
				SET d.REGLA_GENERAL_14B = CASE
				                            WHEN d.VAL_Vigencias_EjecGastos_14B IS NULL AND d.VAL_Vigencias_ProgGastos_14B IS NULL THEN 'SIN DATOS'
				                            WHEN d.VAL_Vigencias_EjecGastos_14B IS NULL THEN 'NO CUMPLE'
				                            WHEN d.VAL_Vigencias_ProgGastos_14B IS NULL THEN 'NO CUMPLE'
				                            WHEN d.VAL_Vigencias_EjecGastos_14B = d.VAL_Vigencias_ProgGastos_14B THEN 'CUMPLE'
				                            ELSE 'NO CUMPLE'
				                         END,
				    d.ALERTA_14B = CASE
				                      WHEN d.VAL_Vigencias_EjecGastos_14B IS NULL AND d.VAL_Vigencias_ProgGastos_14B IS NULL THEN 'ND_CA0065'
				                      WHEN d.VAL_Vigencias_EjecGastos_14B IS NULL THEN 'NO_EG_CA0065'
				                      WHEN d.VAL_Vigencias_ProgGastos_14B IS NULL THEN 'NO_PG_CA0065'
				                      WHEN d.VAL_Vigencias_EjecGastos_14B = d.VAL_Vigencias_ProgGastos_14B THEN 'OK'
				                      ELSE 'CA0065'
				                  END
				FROM GENERAL_RULES_DATA d
				""";

		jdbcTemplate.execute(updateRegla14B);

	}

	public void applyGeneralRule14A() {
		UtilsDB.ensureColumnsExist(
				"GENERAL_RULES_DATA",
				"REGLA_GENERAL_14A:NVARCHAR(20)",
				"ALERTA_14A:NVARCHAR(20)");

		String queryOptimizada = String.format(
				"""
						UPDATE d
						SET
						    REGLA_GENERAL_14A = CASE
						        WHEN ejec.ID IS NOT NULL AND prog.ID IS NOT NULL THEN 'CUMPLE'
						        ELSE 'NO CUMPLE'
						    END,
						    ALERTA_14A = CASE
						        WHEN ejec.ID IS NULL AND prog.ID IS NOT NULL THEN 'CA0064_NE'
						        WHEN ejec.ID IS NOT NULL AND prog.ID IS NULL THEN 'CA0064_NP'
						        WHEN ejec.ID IS NULL AND prog.ID IS NULL THEN 'CA0064'
						        ELSE 'OK'
						    END
						FROM GENERAL_RULES_DATA d
						LEFT JOIN (
						    SELECT DISTINCT
						        FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
						        1 AS ID
						    FROM %s WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE COD_VIGENCIA_DEL_GASTO IN ('1', '1.0')
						) ejec
						    ON ejec.FECHA = d.FECHA
						    AND ejec.TRIMESTRE = d.TRIMESTRE
						    AND ejec.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
						    AND ejec.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
						LEFT JOIN (
						    SELECT DISTINCT
						        FECHA, TRIMESTRE, CODIGO_ENTIDAD_INT, AMBITO_CODIGO_STR,
						        1 AS ID
						    FROM %s WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE COD_VIGENCIA_DEL_GASTO IN ('1', '1.0')
						) prog
						    ON prog.FECHA = d.FECHA
						    AND prog.TRIMESTRE = d.TRIMESTRE
						    AND prog.CODIGO_ENTIDAD_INT = d.CODIGO_ENTIDAD
						    AND prog.AMBITO_CODIGO_STR = d.AMBITO_CODIGO
						""",
				TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS,
				TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
		jdbcTemplate.execute(queryOptimizada);

		String updateNoDataEjecGastos = String.format(
				"""
						UPDATE GENERAL_RULES_DATA
						SET REGLA_GENERAL_14A = 'SIN DATOS',
						    ALERTA_14A = 'NO_EG'
						WHERE NOT EXISTS (
						    SELECT 1
						    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
						      AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
						      AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
						      AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
						)
						""", TABLA_EJEC_GASTOS, TABLA_EJEC_GASTOS);
		jdbcTemplate.update(updateNoDataEjecGastos);

		String updateNoDataProgGastos = String.format(
				"""
						UPDATE GENERAL_RULES_DATA
						SET REGLA_GENERAL_14A = 'SIN DATOS',
						    ALERTA_14A = 'NO_PG'
						WHERE NOT EXISTS (
						    SELECT 1
						    FROM %s a WITH (INDEX(IDX_%s_COMPUTED))
						    WHERE a.FECHA = GENERAL_RULES_DATA.FECHA
						      AND a.TRIMESTRE = GENERAL_RULES_DATA.TRIMESTRE
						      AND a.CODIGO_ENTIDAD_INT = GENERAL_RULES_DATA.CODIGO_ENTIDAD
						      AND a.AMBITO_CODIGO_STR = GENERAL_RULES_DATA.AMBITO_CODIGO
						)
						""", TABLA_PROG_GASTOS, TABLA_PROG_GASTOS);
		jdbcTemplate.update(updateNoDataProgGastos);

	};

}
