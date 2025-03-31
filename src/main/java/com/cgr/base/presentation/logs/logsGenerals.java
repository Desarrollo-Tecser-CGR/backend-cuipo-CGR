package com.cgr.base.presentation.logs;

import java.time.LocalDateTime;
import java.util.List;

import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.logs.service.LogGeneralService;
import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;
import com.cgr.base.presentation.controller.AbstractController;

// Asegúrate de importar tus clases y métodos auxiliares, por ejemplo, requestResponse(...)

@PreAuthorize("hasAuthority('MENU_7')")
@RestController
@RequestMapping("/api/v1/logs")
public class logsGenerals extends AbstractController {

    private final LogGeneralService logService;

    public logsGenerals(LogGeneralService logService) {
        this.logService = logService;
    }

    @GetMapping("/filtered")
    public ResponseEntity<?> getLogAll(
            @RequestParam(value = "userId", required = false) Long userId,
            @RequestParam(value = "logType", required = false) String logTypeStr,
            @RequestParam(value = "detail", required = false) String detail,
            @RequestParam(value = "createdAt", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") LocalDateTime createdAt) {

        // Convertir logTypeStr a enum (si se envía)
        LogType logType = null;
        if (logTypeStr != null && !logTypeStr.trim().isEmpty()) {
            try {
                logType = LogType.valueOf(logTypeStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                return requestResponse(null, "Parámetro 'logType' inválido.", HttpStatus.BAD_REQUEST, false);
            }
        }

        List<LogsEntityGeneral> logs = logService.findLogsByFilters(userId, logType, detail, createdAt);
        return requestResponse(logs, "Login Logs filtrados.", HttpStatus.OK, true);
    }
}
