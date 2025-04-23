package com.cgr.base.application.controller;

import java.util.Date;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.services.logs.exit.LogExitService;
import com.cgr.base.domain.dto.dtoLogs.logsExit.LogExitDto;
import com.cgr.base.domain.dto.dtoLogs.logsExit.LogExtiRequestDto;

import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("api/v1/logsExit")
public class LogsExitController extends AbstractController {

    //
    private LogExitService logExitService;

    @GetMapping("/{user}")
    public ResponseEntity<?> getLastByUser(@PathVariable String user) {

        LogExitDto logExitDto = this.logExitService.getLastLog(user);

        if (logExitDto == null) {
            return requestResponse(null, "No existen registros de salida para el usuario", HttpStatus.OK, true);
        }

        return requestResponse(logExitDto, "ultima salida del usuario", HttpStatus.OK, true);
    }

    @PostMapping
    public ResponseEntity<?> createExitLog(@RequestBody LogExtiRequestDto user) {

        LogExitDto logExitDto = this.logExitService.saveLogExit(user.getName(), new Date());

        if (logExitDto == null) {
            return requestResponse(null, "No existe el usuario en el sistema", HttpStatus.OK, true);
        }

        return requestResponse(logExitDto, "Log de salida creado con éxito", HttpStatus.OK, true);

    }

}
