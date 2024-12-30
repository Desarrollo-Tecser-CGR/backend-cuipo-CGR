package com.cgr.base.application.GeneralRules;

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
    private OpenDataProgIngRepository openDataProgIngRepository;

    @Autowired
    private GeneralRulesRepository generalRulesRepository;

    // Transferencia del Nombre de Cuenta
    @Transactional
    public void transferAccountNameToGeneralRules() {
        
        List<OpenDataProgIng> openDataList = openDataProgIngRepository.findAll();

        openDataList.forEach(openData -> {
            GeneralRulesEntity generalRulesEntity = new GeneralRulesEntity();
            generalRulesEntity.setAccountName(openData.getNombreCuenta());
            generalRulesRepository.save(generalRulesEntity);
        });
    }

    // Regla1: Validacion presupuesto definitivo.
    /*
     Transactional
    public void applyGeneralRule1() {
        
        List<OpenDataProgIng> openDataList = openDataProgIngRepository.findAll();
        openDataList.forEach(openData -> {
            String AccountName = openData.getNombreCuenta();
            
            String generalRule2Value = AccountName != null && AccountName.toLowerCase().contains("liquidacion") 
                                    ? "NO CUMPLE" : "CUMPLE";

            GeneralRulesEntity generalRulesEntity = generalRulesRepository
            .findByAccountName(AccountName)
            .orElse(new GeneralRulesEntity());
            
            generalRulesEntity.setGeneralRule2(generalRule2Value);
            generalRulesRepository.save(generalRulesEntity);

        });
    }
     */
    @Transactional
    public void applyGeneralRule1() {
        // Obtener todos los datos
        List<OpenDataProgIng> openDataList = openDataProgIngRepository.findAll();
        
        // Solo procesar los primeros 3 elementos
        List<OpenDataProgIng> firstThreeOpenDataList = openDataList.size() > 3 ? openDataList.subList(0, 3) : openDataList;
    
        firstThreeOpenDataList.forEach(openData -> {
            // Obtener el nombre de la cuenta
            String AccountName = openData.getNombreCuenta();
            
            // Imprimir por consola el nombre de la cuenta procesada
            System.out.println("Procesando cuenta: " + AccountName);
    
            // Determinar el valor de la regla general 2
            String generalRule2Value = AccountName != null && AccountName.toLowerCase().contains("liquidacion") 
                                        ? "NO CUMPLE" : "CUMPLE";
            
            // Imprimir el valor de la regla
            System.out.println("Valor de la regla general 2: " + generalRule2Value);
    
            // Buscar o crear una entidad para la cuenta
            GeneralRulesEntity generalRulesEntity = generalRulesRepository
                .findByAccountName(AccountName)
                .orElse(new GeneralRulesEntity());
    
            // Asignar el valor a la regla general 2
            generalRulesEntity.setGeneralRule2(generalRule2Value);
    
            // Imprimir por consola antes de guardar
            System.out.println("Guardando regla general para cuenta: " + AccountName);
    
            // Guardar la entidad
            generalRulesRepository.save(generalRulesEntity);
    
            // Imprimir confirmación de guardado
            System.out.println("Regla general guardada para cuenta: " + AccountName);
        });
    }
    
    
    @Transactional
    public void applyGeneralRule2() {
        
        List<OpenDataProgIng> openDataList = openDataProgIngRepository.findAll();
        openDataList.forEach(openData -> {
            Double value = openData.getPresupuestoDefinitivo();
            
            if (value == null || value.isNaN()) {
                value = 0.0;
            }

            String generalRule1Value = value > 100000000 ? "CUMPLE" : "NO CUMPLE";
            
            GeneralRulesEntity generalRulesEntity = generalRulesRepository
                .findByAccountName(openData.getNombreCuenta())
                .orElse(new GeneralRulesEntity());
            
            generalRulesEntity.setGeneralRule1(generalRule1Value);
            generalRulesRepository.save(generalRulesEntity);

        });
    }

    // Data GeneralRules por AccountName
    @Transactional
    public List<GeneralRulesEntity> getGeneralRulesData() {
        transferAccountNameToGeneralRules();
        return generalRulesRepository.findAll();
    }

}

