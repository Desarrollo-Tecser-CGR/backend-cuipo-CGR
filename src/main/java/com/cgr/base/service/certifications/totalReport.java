package com.cgr.base.service.certifications;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

@Service
public class totalReport {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    public List<Map<String, Object>> getCertificationStats() {
        String sql = "SELECT " +
                "FECHA, " +
                "SUM(CASE WHEN ESTADO_CALIDAD = 'CERTIFICA' THEN 1 ELSE 0 END) AS CALIDAD_CUMPLE, " +
                "SUM(CASE WHEN ESTADO_CALIDAD = 'NO CERTIFICA' THEN 1 ELSE 0 END) AS CALIDAD_NO_CUMPLE, " +
                "SUM(CASE WHEN ESTADO_L617 = 'CERTIFICA' THEN 1 ELSE 0 END) AS L617_CUMPLE, " +
                "SUM(CASE WHEN ESTADO_L617 = 'NO CERTIFICA' THEN 1 ELSE 0 END) AS L617_NO_CUMPLE " +
                "FROM " + DATASOURCE_NAME + ".dbo.CONTROL_CERTIFICACION " +
                "GROUP BY FECHA " +
                "ORDER BY FECHA DESC";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        List<Map<String, Object>> certificationStats = new ArrayList<>();

        for (Object[] row : results) {
            Map<String, Object> yearData = new HashMap<>();

            // Extract the year
            Integer year = (row[0] != null) ? ((Number) row[0]).intValue() : null;
            yearData.put("FECHA", year);

            // Calidad details
            Map<String, Object> calidadDetails = new HashMap<>();
            calidadDetails.put("CUMPLE", row[1] != null ? ((Number) row[1]).intValue() : 0);
            calidadDetails.put("NO_CUMPLE", row[2] != null ? ((Number) row[2]).intValue() : 0);
            yearData.put("CALIDAD", calidadDetails);

            // L617 details
            Map<String, Object> l617Details = new HashMap<>();
            l617Details.put("CUMPLE", row[3] != null ? ((Number) row[3]).intValue() : 0);
            l617Details.put("NO_CUMPLE", row[4] != null ? ((Number) row[4]).intValue() : 0);
            yearData.put("L617", l617Details);

            certificationStats.add(yearData);
        }

        return certificationStats;
    }

}
