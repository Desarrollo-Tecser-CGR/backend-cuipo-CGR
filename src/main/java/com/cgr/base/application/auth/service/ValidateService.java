package com.cgr.base.application.auth.service;

import org.springframework.stereotype.Service;

import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.application.auth.dto.UserDto;
import com.cgr.base.application.auth.mapper.AuthMapper;
import com.cgr.base.infrastructure.persistence.entity.RoleEntity;
import com.cgr.base.infrastructure.persistence.entity.UserEntity;
import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
@AllArgsConstructor
@Slf4j
public class ValidateService {

    private final JwtService jwtService;

    private final IUserRepositoryJpa userRepositoryFull;

    public Map<String, Object> validationToken(String token) {
        Map<String, Object> response = new HashMap<>();
        AuthResponseDto userDto = new AuthResponseDto();

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
                return response;
            }

            UserEntity user = this.userRepositoryFull
                    .findBySAMAccountNameWithRoles(jwtService.getClaimUserName(token)).get();

            userDto.setSAMAccountName(jwtService.getClaimUserName(token));
            userDto.setIsEnable(true);

            userDto.setRoles(user.getRoles().stream().map(RoleEntity::getName).toList());

            String newToken = jwtService.createToken(userDto, user.getRoles());

            List<Menu> menus = this.userRepositoryFull
                    .findMenusByRoleNames(user.getRoles().stream().map(RoleEntity::getName).toList());

            userDto.setMenus(menus);

            userDto.setToken(newToken);
            userDto.setIsEnable(true);

            response.put("user", userDto);
            response.put("message", "Valid token ");
            response.put("statusCode", 200);
            response.put("status", "Succes");

            return response;

        } catch (Exception e) {
            log.info("Error in Jwt validation: " + e.getMessage());
            response.put("message", "An error occurred during token validation");
            response.put("statusCode", 500);
            response.put("status", "error");
            return response;
        }
    }
}
