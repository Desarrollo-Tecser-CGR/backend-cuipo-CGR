package com.test.testactivedirectory.application.auth.service;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.JsonEOFException;
import com.test.testactivedirectory.application.auth.dto.TokenDto;
import com.test.testactivedirectory.application.auth.dto.UserDto;
import com.test.testactivedirectory.infrastructure.security.Jwt.services.JwtService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@AllArgsConstructor
@Slf4j
public class ValidateService {

    private final JwtService jwtService;

    public UserDto validationToken(String token) {
         UserDto userDto = new UserDto();
        try {

            if (jwtService.validateFirma(token) != null)
                return userDto;
            if (jwtService.validateToken(token))
                return userDto;
                
            userDto.setUser(jwtService.getClaimUserName(token));

            return  userDto;

        } catch (Exception e) {
            log.info("error provider Jwt " + e.getMessage());
            return null;
        }

    };

}
