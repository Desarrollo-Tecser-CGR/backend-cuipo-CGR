package com.cgr.base.application.GeneralRules.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;

@Service
public class RuleApplicationService {

    @Autowired
    private GeneralRulesRepository generalRulesRepo;

    @Autowired
    private GeneralRulesEvaluator Evaluator;

    @Transactional
    public void applyRules() {

        List<GeneralRulesEntity> generalRulesData = generalRulesRepo.findAll();

        for (GeneralRulesEntity generalRule : generalRulesData) {

            // Regla 1: Presupuesto Definitivo.
            String resultGeneralRule1 = Evaluator.evaluateGeneralRule1(generalRule.getFinalBudget());
            generalRule.setGeneralRule1(resultGeneralRule1);

            // Regla2: Entidad en Liquidacion.
            String resultGeneralRule2 = Evaluator.evaluateGeneralRule2(generalRule.getAccountName());
            generalRule.setGeneralRule2(resultGeneralRule2);

            // Regla3: Presupuesto Inicial vs Definitivo.
            String resultGeneralRule3 = Evaluator.evaluateGeneralRule3(generalRule.getFinalBudget(),generalRule.getInitialBudget());
            generalRule.setGeneralRule3(resultGeneralRule3);

        }

        generalRulesRepo.saveAll(generalRulesData);
    }

}
