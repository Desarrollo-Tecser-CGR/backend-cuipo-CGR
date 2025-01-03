package com.cgr.base.application.GeneralRules;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.OpenDataProgIng;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.OpenDataProgIngRepository;

@Service
public class GeneralRulesManager {

    @Autowired
    private GeneralRulesRepository generalRulesRepository;

    @Autowired
    private OpenDataProgIngRepository openDataProgIngRepository;

    @Transactional
    public void transferDataGeneralRules() {

        List<GeneralRulesEntity> existingEntries = generalRulesRepository.findAll();
        List<GeneralRulesEntity> newEntities = new ArrayList<>();

        List<OpenDataProgIng> progIngList = openDataProgIngRepository.findAll();

        for (OpenDataProgIng openData : progIngList) {
            GeneralRulesEntity newEntity = new GeneralRulesEntity();

            newEntity.setPeriod(extractYearPeriod(openData.getPeriodo()));
            newEntity.setEntityAmbit(openData.getNombreAmbito());
            newEntity.setEntityName(openData.getNombreEntidad());
            newEntity.setAccountName(openData.getNombreCuenta());

            boolean isDuplicate = existingEntries.stream()
                    .anyMatch(existing
                            -> areFieldsEqual(existing.getAccountName(), newEntity.getAccountName())
                    && areFieldsEqual(existing.getEntityAmbit(), newEntity.getEntityAmbit())
                    && areFieldsEqual(existing.getEntityName(), newEntity.getEntityName())
                    && areFieldsEqual(existing.getPeriod(), newEntity.getPeriod())
                    );

            if (!isDuplicate) {

                newEntities.add(newEntity);
            }

        }

        if (!newEntities.isEmpty()) {

            generalRulesRepository.saveAll(newEntities);
        }

    }

    private boolean areFieldsEqual(String field1, String field2) {
        if (field1 == null && field2 == null) {
            return true;
        }
        if (field1 == null || field2 == null) {
            return false;
        }
        return field1.trim().equalsIgnoreCase(field2.trim());
    }

    private String extractYearPeriod(String period) {
        return period.length() >= 4 ? period.substring(0, 4) : period;
    }

    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferDataGeneralRules();
        return generalRulesRepository.findAll();
    }

}
