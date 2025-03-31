package com.cgr.base.application.services.auth.service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.stereotype.Service;

import com.cgr.base.application.services.logs.exit.LogExitService;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@AllArgsConstructor
@Slf4j
public class ValidateService {

    private final JwtService jwtService;

    private final LogExitService logExitService;

    public Map<String, Object> validationToken(String token) {
        Map<String, Object> response = new HashMap<>();

        try {
            if (jwtService.validateFirma(token) != null) {
                response.put("message", "Token Invalid");
                response.put("statusCode", 498);
                response.put("status", "Error");
                return response;
            }

            if (jwtService.validateToken(token)) {
                response.put("message", "Token expired/invalid");
                response.put("statusCode", 498);
                response.put("status", "Error");
                String user = jwtService.getClaimUserName(token);

                Date expirationDate = jwtService.extractExpiration(token);

                this.logExitService.saveLogExit(user, expirationDate);

                return response;

            } else {
                response.put("message", "Token valid");
                response.put("statusCode", 200);
                response.put("status", "success");

                return response;
            }

        } catch (Exception e) {
            log.info("Error in Jwt validation: " + e.getMessage());
            response.put("message", "An error occurred during token validation");
            response.put("statusCode", 500);
            response.put("status", "error");
            return response;
        }
    }
}
