package com.cgr.base.application.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.services.entityProvitionalPlan.service.EntityProvitionalPlanService;

@RestController
@RequestMapping("/api/v1/EntitysProv")
public class EntityProvitionalPlanController extends AbstractController {

    @Autowired
    private EntityProvitionalPlanService entityProvitionalPlanService;

    @GetMapping
    public ResponseEntity<?> getAll() {
        return requestResponse(this.entityProvitionalPlanService.findAllEntitysProvitionalPlan(),
                "Entidades del plan provisonal", HttpStatus.OK, true);
    }

}
