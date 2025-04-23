package com.cgr.base.controller.logs;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.dto.logs.LogWithUserFullNameDTO;
import com.cgr.base.entity.logs.LogType;
import com.cgr.base.service.logs.LogGeneralService;

// Asegúrate de importar tus clases y métodos auxiliares, por ejemplo, requestResponse(...)

@PreAuthorize("hasAuthority('MENU_LOGS')")
@RestController
@RequestMapping("/api/v1/logs")
public class logsGenerals extends AbstractController {

    @Autowired
    private final LogGeneralService logService;

    public logsGenerals(LogGeneralService logService) {
        this.logService = logService;
    }

    @GetMapping("/filtered")
    public ResponseEntity<?> getLogAll(
            @RequestParam(value = "userId", required = false) Long userId,
            @RequestParam(value = "logType", required = false) String logTypeStr,
            @RequestParam(value = "detail", required = false) String detail,
            @RequestParam(value = "createdAt", required = false) String create_date) {

        // Convertir logTypeStr a enum (si se envía)
        LogType logType = null;
        if (logTypeStr != null && !logTypeStr.trim().isEmpty()) {
            try {
                logType = LogType.valueOf(logTypeStr.toUpperCase());
            } catch (IllegalArgumentException e) {
                return requestResponse(null, "Parámetro 'logType' inválido.", HttpStatus.BAD_REQUEST, false);
            }
        }

        List<LogWithUserFullNameDTO> logs = logService.findLogsByFilters(userId, logType, detail, create_date);
        return requestResponse(logs, "Login Logs filtrados.", HttpStatus.OK, true);

    }
}
