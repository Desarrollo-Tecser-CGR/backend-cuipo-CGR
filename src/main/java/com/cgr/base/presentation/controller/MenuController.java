package com.cgr.base.presentation.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.auth.service.MenuService;

@RestController
@RequestMapping("/api/v1/menu")
public class MenuController {

    @Autowired
    private MenuService menuTest;  

    @GetMapping("/health")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Service is Running!");
    }

    @GetMapping("/list")
    public ResponseEntity<?> getMenus() {
        return ResponseEntity.ok(menuTest.getMenus());
    }

}
