package com.test.testactivedirectory.presentation.controller.rulesController;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.test.testactivedirectory.application.Rules.RuleEngine;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.InfoGeneralRepository;

@RestController
@RequestMapping("/api/rules")
public class RulesController {

    @Autowired
    private RuleEngine ruleEngine;

    @GetMapping()
    public Map<String, Object> getAll() {
        return this.ruleEngine.implementRules();
    }

}
