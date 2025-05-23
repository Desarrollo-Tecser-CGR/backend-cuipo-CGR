package com.cgr.base.controller.auth;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.dto.auth.AuthRequestDto;
import com.cgr.base.service.auth.AuthService;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/auth")
public class AuthController extends AbstractController {

    @Autowired
    AuthService authService;

    @GetMapping("/health")
    public ResponseEntity<String> checkHealth() {
        return ResponseEntity.ok("Service is Running!");
    }

    @PostMapping("/loginActiveDirectory")
    public ResponseEntity<?> loginActiveDirectory(@RequestBody AuthRequestDto request,
            final HttpServletRequest servletRequest) throws JsonProcessingException {

        return requestResponse(
                authService.authWithLDAPActiveDirectory(request, servletRequest),
                "User authenticated successfully",
                HttpStatus.OK,
                true);
    }

}