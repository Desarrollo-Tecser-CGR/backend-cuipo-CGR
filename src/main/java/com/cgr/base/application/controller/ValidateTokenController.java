package com.cgr.base.application.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.cgr.base.domain.dto.dtoAuth.TokenDto;
import com.cgr.base.application.services.auth.service.ValidateService;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/auth")
public class ValidateTokenController extends AbstractController {
    private ValidateService validateService;

    @PostMapping("/verificarToken")
    public ResponseEntity<?> validateEmailToken(@Valid @RequestBody TokenDto tokenDto, BindingResult result) {
        return requestResponse(result, () -> validateService.validationToken(tokenDto.getToken()), "Token valido", HttpStatus.OK, true);
    }

}
