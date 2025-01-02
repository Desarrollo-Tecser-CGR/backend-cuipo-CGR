package com.test.testactivedirectory.presentation.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.test.testactivedirectory.application.auth.dto.AuthRequestDto;
import com.test.testactivedirectory.application.auth.dto.TokenDto;
import com.test.testactivedirectory.application.auth.dto.UserDto;
import com.test.testactivedirectory.application.auth.service.ValidateService;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/verificarToken/email")
public class ValidateTokenController {
    private ValidateService validateService;

    @PostMapping("/verify")
    public ResponseEntity<UserDto> validateEmailToken(@RequestBody TokenDto tokenDto) {
        return ResponseEntity.ok(validateService.validationToken(tokenDto.getToken()));
    }

}
