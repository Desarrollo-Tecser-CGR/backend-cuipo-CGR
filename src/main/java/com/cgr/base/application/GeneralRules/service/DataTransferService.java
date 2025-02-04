package com.cgr.base.application.GeneralRules.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataEjecGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.EjecGastosRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgGastosRepo;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.ProgIngresosRepo;

@Service
public class DataTransferService {

    @Autowired
    private GeneralRulesRepository generalRulesRepo;

    @Autowired
    private ProgIngresosRepo ProgIngresosRepo;

    @Autowired
    private ProgGastosRepo ProgGastosRepo;

    @Autowired
    private EjecGastosRepo EjecGastosRepo;

    @Async
    @Transactional
    public CompletableFuture<Void> transferDataGeneralRules() {
        Set<String> existingKeys = new HashSet<>();
        generalRulesRepo.findAll().forEach(entry -> existingKeys.add(generateKey(entry)));

        List<GeneralRulesEntity> newEntities = new ArrayList<>();

        processData(ProgIngresosRepo.findAll(), newEntities, existingKeys);
        processData(ProgGastosRepo.findAll(), newEntities, existingKeys);
        processData(EjecGastosRepo.findAll(), newEntities, existingKeys);

        saveInBatches(newEntities);

        return CompletableFuture.completedFuture(null);
    }

    private void processData(List<?> dataList, List<GeneralRulesEntity> newEntities, Set<String> existingKeys) {
        for (Object data : dataList) {
            GeneralRulesEntity newEntity = mapToGeneralRulesEntity(data);
            String key = generateKey(newEntity);

            if (!existingKeys.contains(key)) {
                newEntities.add(newEntity);
                existingKeys.add(key);
            }
        }
    }

    private GeneralRulesEntity mapToGeneralRulesEntity(Object data) {
        GeneralRulesEntity entity = new GeneralRulesEntity();

        if (data instanceof DataProgIngresos) {
            DataProgIngresos d = (DataProgIngresos) data;
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        } else if (data instanceof DataProgGastos) {
            DataProgGastos d = (DataProgGastos) data;
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        } else if (data instanceof DataEjecGastos) {
            DataEjecGastos d = (DataEjecGastos) data;
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        }

        return entity;
    }

    private String generateKey(GeneralRulesEntity entity) {
        return entity.getEntityName() + ":" + entity.getAccountName() + ":" + entity.getYear() + ":" + entity.getPeriod() + ":" + entity.getNameAmbit();
    }
    

    private void saveInBatches(List<GeneralRulesEntity> entities) {
        int batchSize = 500;
        for (int i = 0; i < entities.size(); i += batchSize) {
            int end = Math.min(i + batchSize, entities.size());
            generalRulesRepo.saveAll(entities.subList(i, end));
        }
    }

    private String extractYear(String periodo) {
        return periodo != null && periodo.length() >= 4 ? periodo.substring(0, 4) : "";
    }

    private String extractPeriod(String periodo) {
        if (periodo != null && periodo.length() >= 6) {
            String month = periodo.substring(4, 6);
            switch (month) {
                case "01":
                case "02":
                case "03":
                    return "3";
                case "04":
                case "05":
                case "06":
                    return "6";
                case "07":
                case "08":
                case "09":
                    return "9";
                case "10":
                case "11":
                case "12":
                    return "12";
                default:
                    return "";
            }
        }
        return "";
    }

}
