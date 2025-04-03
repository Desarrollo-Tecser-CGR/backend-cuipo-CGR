package com.cgr.base.application.controller;

import com.cgr.base.application.services.logs.ingress.service.LogService;
import com.cgr.base.domain.dto.dtoLogs.MonthlyLoginCounts; // Import del DTO correcto
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/logs")
public class LogController {

    @Autowired
    private LogService logService;

    @GetMapping("/counts")
    public ResponseEntity<MonthlyLoginCounts> getLoginCounts() {
        MonthlyLoginCounts counts = logService.countSuccessfulAndFailedLogins();
        return ResponseEntity.ok(counts);
    }

    @GetMapping("/counts/month")
    public ResponseEntity<List<MonthlyLoginCounts>> getLoginCountsByMonth(
            @RequestParam("year") int year) {
        List<MonthlyLoginCounts> counts = logService.countSuccessfulAndFailedLoginsByMonth(year);
        return ResponseEntity.ok(counts);
    }

    @GetMapping("/counts/year")
    public ResponseEntity<MonthlyLoginCounts> getLoginCountsByYear(@RequestParam("year") int year) {
        MonthlyLoginCounts counts = logService.countSuccessfulAndFailedLoginsByYear(year);
        return ResponseEntity.ok(counts);
    }
}