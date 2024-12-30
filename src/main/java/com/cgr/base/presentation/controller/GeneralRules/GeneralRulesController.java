package com.cgr.base.presentation.controller.GeneralRules;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.GeneralRules.GeneralRulesManager;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;

@RestController
public class GeneralRulesController {

    @Autowired
    private GeneralRulesManager generalRulesManager;

    @GetMapping("/api/v1/auth/general-rules")
    public List<GeneralRulesEntity> getGeneralRules() {
        return generalRulesManager.getGeneralRulesData();
    }
    
}
