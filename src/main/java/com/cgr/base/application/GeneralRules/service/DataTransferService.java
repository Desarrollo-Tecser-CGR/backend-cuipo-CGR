package com.cgr.base.application.GeneralRules.service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.GeneralRules.mapper.mapperEntity;
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
    private ProgIngresosRepo progIngresosRepo;

    @Autowired
    private ProgGastosRepo progGastosRepo;

    @Autowired
    private EjecGastosRepo ejecGastosRepo;

    @Autowired
    private mapperEntity Mapper;

    // Llamada Automatica Transferencia Datos
    @Async
    @Transactional
    @Scheduled(cron = "0 0 1 * * ?")
    public CompletableFuture<Void> scheduledTransfer() {
        performDataTransfer();
        return CompletableFuture.completedFuture(null);
    }

    // Llamada Manual Transferencia Datos
    @Transactional
    public void transferDataGeneralRules() {
        performDataTransfer();
    }

    // Logica Transferencia Datos
    private void performDataTransfer() {
        Set<String> existingKeys = new HashSet<>();
        generalRulesRepo.findAll().forEach(entry -> existingKeys.add(Mapper.generateKeyPeriod(entry)));

        List<GeneralRulesEntity> newEntities = new ArrayList<>();
        processData(progIngresosRepo.findAll(), newEntities, existingKeys);
        processData(progGastosRepo.findAll(), newEntities, existingKeys);
        processData(ejecGastosRepo.findAll(), newEntities, existingKeys);

        saveInBatches(newEntities);
    }

    private void processData(List<?> dataList, List<GeneralRulesEntity> newEntities, Set<String> existingKeys) {
        for (Object data : dataList) {
            GeneralRulesEntity newEntity = Mapper.mapToGeneralRulesEntity(data);
            String key = Mapper.generateKeyPeriod(newEntity);

            if (!existingKeys.contains(key)) {
                newEntities.add(newEntity);
                existingKeys.add(key);
            }
        }
    }

    private void saveInBatches(List<GeneralRulesEntity> entities) {
        int batchSize = 500;
        for (int i = 0; i < entities.size(); i += batchSize) {
            int end = Math.min(i + batchSize, entities.size());
            generalRulesRepo.saveAll(entities.subList(i, end));
        }
    }

}
