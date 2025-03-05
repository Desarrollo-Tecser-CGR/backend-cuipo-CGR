package com.cgr.base.application.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import com.cgr.base.application.auth.dto.TokenDto;
import com.cgr.base.application.auth.service.ValidateService;

import jakarta.validation.Valid;
import lombok.AllArgsConstructor;

@RestController
@AllArgsConstructor
@RequestMapping("/api/v1/token")
public class ValidateTokenController extends AbstractController {
    private ValidateService validateService;

    @PostMapping()
    public ResponseEntity<?> validateEmailToken(@Valid @RequestBody TokenDto tokenDto, BindingResult result) {
        return requestResponse(result, () -> validateService.validationToken(tokenDto.getToken()), "Token valido",
                HttpStatus.OK, true);
    }

}
