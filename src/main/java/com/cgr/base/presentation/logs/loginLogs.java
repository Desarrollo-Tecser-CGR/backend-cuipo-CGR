package com.cgr.base.presentation.logs;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.logs.usecase.ILogUseCase;
import com.cgr.base.presentation.controller.AbstractController;

@PreAuthorize("hasAuthority('MENU_7')")
@RestController
@RequestMapping("/api/v1/log")
public class loginLogs extends AbstractController {

    private final ILogUseCase logService;

    public loginLogs(ILogUseCase logService) {
        this.logService = logService;
    }

    @GetMapping("/login")
    public ResponseEntity<?> getLogAll() {
        return requestResponse(this.logService.logFindAll(), "Login Logs.", HttpStatus.OK, true);
    }

}
