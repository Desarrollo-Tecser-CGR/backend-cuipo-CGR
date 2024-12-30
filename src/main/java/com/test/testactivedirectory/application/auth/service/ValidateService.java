package com.test.testactivedirectory.application.auth.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.test.testactivedirectory.application.auth.dto.TokenDto;
import com.test.testactivedirectory.infrastructure.security.Jwt.services.JwtService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@AllArgsConstructor
@Slf4j
public class ValidateService {

    private final JwtService jwtService;

    public TokenDto validationToken(String token) {
        try {

            if (jwtService.validateFirma(token) != null)
                return new TokenDto("");
            if (jwtService.validateToken(token))
                return new TokenDto("");

            return new TokenDto(token);

        } catch (Exception e) {
            log.info("error provider Jwt " + e.getMessage());
            return null;
        }

    };

}
