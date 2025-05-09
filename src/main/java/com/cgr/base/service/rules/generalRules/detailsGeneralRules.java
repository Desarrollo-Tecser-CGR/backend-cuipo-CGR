package com.cgr.base.service.rules.generalRules;

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

    public GeneralRulesNames detailsGeneralRequest(Map<String, String> filters) {

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

    public GeneralRulesNames getFilteredGeneralData(String fecha, String trimestre, String ambito,
            String entidad, String codigoRegla) {

        Optional<GeneralRulesNames> optionalRegla = GeneralRepo.findByCodigoRegla(codigoRegla);

        if (optionalRegla.isEmpty()) {
            throw new IllegalArgumentException("Código de regla no válido o no encontrado: " + codigoRegla);
        }

        GeneralRulesNames regla = optionalRegla.get();

        if (regla.getDetalles() == null || "0".equals(regla.getDetalles().trim())) {
            throw new IllegalStateException("La regla seleccionada no tiene detalles asociados.");
        }

        // A partir de aquí, trabajaremos con regla.getCodigoTabla(),
        // regla.getDetalles(), etc.

        return regla; // temporal hasta que definamos el siguiente paso
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
