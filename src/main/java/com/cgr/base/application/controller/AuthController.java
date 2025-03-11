package com.cgr.base.application.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoAuth.UserAuthDto;
import com.cgr.base.application.services.auth.usecase.IAuthUseCase;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private final IAuthUseCase authUseCase;

    public AuthController(IAuthUseCase authUseCase) {
        this.authUseCase = authUseCase;
    }

    @GetMapping("/health")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Service is running!");
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody AuthRequestDto request, final HttpServletRequest servletRequest)
            throws JsonProcessingException {

        return ResponseEntity.ok(authUseCase.signIn(request, servletRequest));

    }

    @PostMapping("/loginActiveDirectory")
    public ResponseEntity<?> loginActiveDirectory(@RequestBody AuthRequestDto request,
            final HttpServletRequest servletRequest) throws JsonProcessingException {
        return ResponseEntity.ok(authUseCase.authWithLDAPActiveDirectory(request, servletRequest));

    }

    @PostMapping("/tokenEmail")
    public ResponseEntity<?> tokenEmail(@RequestBody UserAuthDto request, final HttpServletRequest servletRequest)
            throws JsonProcessingException {
        return ResponseEntity.ok(authUseCase.emailLogin(request));

    }

}
