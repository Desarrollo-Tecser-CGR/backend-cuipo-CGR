package com.cgr.base.application.controller;

import com.cgr.base.application.services.logs.ingress.service.LogService;
import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoLogs.LoginCountDto;
import com.cgr.base.domain.dto.dtoLogs.MonthlyLoginCounts; // Import del DTO correcto
import com.cgr.base.domain.models.entity.Logs.LogEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Optional;

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




    @GetMapping("/estadisticas/{correo}")
    public ResponseEntity<LoginCountDto> getLoginStats(@PathVariable String correo) {
        LoginCountDto stats = logService.countLoginsByCorreo(correo);
        return ResponseEntity.ok(stats);
    }
}


