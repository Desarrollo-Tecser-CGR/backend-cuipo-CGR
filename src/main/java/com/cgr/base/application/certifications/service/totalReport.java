package com.cgr.base.application.certifications.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

@Service
public class totalReport {

    @PersistenceContext
    private EntityManager entityManager;

    public List<Map<String, Object>> getCertificationStats() {
        String sql = "SELECT FECHA, " +
                "SUM(CASE WHEN ESTADO_CALIDAD = 'CERTIFICA' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS porcentaje_calidad, "
                +
                "SUM(CASE WHEN ESTADO_L617 = 'CERTIFICA' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS porcentaje_l617 " +
                "FROM cuipo_dev.dbo.CONTROL_CERTIFICACION " +
                "GROUP BY FECHA " +
                "ORDER BY FECHA DESC";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        List<Map<String, Object>> certificationStats = new ArrayList<>();

        for (Object[] row : results) {
            Integer year = (row[0] != null) ? ((Number) row[0]).intValue() : null;
            Double percentageQuality = (row[1] != null) ? ((Number) row[1]).doubleValue() : 0.0;
            Double percentageL617 = (row[2] != null) ? ((Number) row[2]).doubleValue() : 0.0;

            Map<String, Object> yearData = new HashMap<>();
            yearData.put("FECHA", year);
            yearData.put("PORCENTAJE_CALIDAD", percentageQuality);
            yearData.put("PORCENTAJE_L617", percentageL617);

            certificationStats.add(yearData);
        }

        return certificationStats;
    }

}
