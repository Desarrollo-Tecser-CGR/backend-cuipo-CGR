package com.cgr.base.controller.logs;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.service.logs.LogService;

@PreAuthorize("hasAuthority('MENU_Logs')")
@RestController
@RequestMapping("/api/v1/log")
public class loginLogs extends AbstractController {

    @Autowired
    private LogService LogService;

    @GetMapping("/login")
    public ResponseEntity<?> getLogAll() {
        var logs = this.LogService.getAllLogsDesc();

        ResponseEntity<?> response = requestResponse(logs, "Login Logs.", HttpStatus.OK, true);

        return response;
    }



}
