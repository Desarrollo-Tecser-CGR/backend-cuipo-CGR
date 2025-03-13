package com.cgr.base.application.rulesEngine;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.repository.rulesEngine.generalRulesRepo;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class specificParameter {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    generalRulesRepo GeneralRepo;

    @Async
    @Transactional
    public void tableSpecificRulesName() {


    }
    
}
