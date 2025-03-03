package com.cgr.base.application.services.auth.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.cgr.base.domain.dto.dtoAuth.AuthResponseDto;
import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Menu.Menu;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@AllArgsConstructor
@Slf4j
public class ValidateService {

    private final JwtService jwtService;

    private final IUserRepositoryJpa userRepositoryFull;

    private final DtoMapper dtoMapper;

    public Map<String, Object> validationToken(String token) {
        Map<String, Object> response = new HashMap<>();
        AuthResponseDto userResponseDto = new AuthResponseDto();

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

            if (user.getEnabled()==true) {
    
                UserDto userDto = this.dtoMapper.convertToDto(user, UserDto.class);

                userResponseDto.setUser(userDto);

                userResponseDto.setIsEnable(true);

                userResponseDto.setRoles(user.getRoles().stream().map(RoleEntity::getName).toList());

                String newToken = jwtService.createToken(userResponseDto, user.getRoles(),3600000);

                List<Menu> menus = this.userRepositoryFull
                                    .findMenusByRoleNames(user.getRoles().stream().map(RoleEntity::getName).toList());

                userResponseDto.setMenus(menus);

                userResponseDto.setToken(newToken);
                userResponseDto.setIsEnable(true);

                response.put("user", userResponseDto);
                response.put("message", "Valid token ");
                response.put("statusCode", 200);
                response.put("status", "Succes");

                return response;
            }
            else{
                response.put("message", "User not enabled");
                response.put("statusCode", 403);
                response.put("status", "disabled");
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
