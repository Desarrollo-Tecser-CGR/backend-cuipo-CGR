package com.cgr.base.application.controller;

import com.cgr.base.application.services.logs.ingress.service.LogService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("api/v1/AuditEntry")
public class LogsLogin
{
    @Autowired
    private LogService logService;

    @GetMapping("/counts")
    public ResponseEntity<String> getLoginCounts() { // Cambiado a String
        String counts = logService.countSuccessfulAndFailedLogins();
        return ResponseEntity.ok(counts);
    }
}
