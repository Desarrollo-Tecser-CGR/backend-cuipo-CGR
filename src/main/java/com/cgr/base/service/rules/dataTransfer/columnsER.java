package com.cgr.base.service.rules.dataTransfer;

import java.util.Arrays;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class columnsER {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String JOIN_CONDITION = "d.FECHA = s.FECHA AND d.TRIMESTRE = s.TRIMESTRE " +
            "AND d.CODIGO_ENTIDAD = s.CODIGO_ENTIDAD AND d.AMBITO_CODIGO = s.AMBITO_CODIGO";

    public void actualizarSpecificRulesData() {

        List<String> sourceTables = Arrays.asList("MEDIDAS_ICLD", "MEDIDAS_GF", "E027", "E028", "E029", "E030", "E031",
                "E032");

        for (String sourceTable : sourceTables) {

            String sqlColumns = "SELECT column_name " +
                    "FROM INFORMATION_SCHEMA.COLUMNS " +
                    "WHERE table_name = ? AND column_name LIKE 'REGLA_ESPECIFICA%'";
            List<String> ruleColumns = jdbcTemplate.queryForList(sqlColumns, String.class, sourceTable);

            for (String ruleColumn : ruleColumns) {
                String sqlCheck = "SELECT COUNT(*) " +
                        "FROM INFORMATION_SCHEMA.COLUMNS " +
                        "WHERE table_name = 'SPECIFIC_RULES_DATA' AND column_name = ?";
                Integer count = jdbcTemplate.queryForObject(sqlCheck, Integer.class, ruleColumn);

                if (count == null || count == 0) {
                    String alterSql = "ALTER TABLE SPECIFIC_RULES_DATA ADD [" + ruleColumn + "] NVARCHAR(255)";
                    jdbcTemplate.execute(alterSql);
                }

                String updateSql = "UPDATE d " +
                        "SET d.[" + ruleColumn + "] = COALESCE(s.[" + ruleColumn + "], 'NO APLICA') " +
                        "FROM SPECIFIC_RULES_DATA d " +
                        "LEFT JOIN " + sourceTable + " s ON " + JOIN_CONDITION;
                jdbcTemplate.execute(updateSql);
            }
        }
    }
}
