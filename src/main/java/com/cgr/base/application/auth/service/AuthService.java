package com.cgr.base.application.auth.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.auth.dto.AuthRequestDto;
import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.application.auth.mapper.AuthMapper;
import com.cgr.base.application.auth.usecase.IAuthUseCase;
import com.cgr.base.application.logs.usecase.ILogUseCase;
import com.cgr.base.domain.models.UserModel;
import com.cgr.base.domain.repository.IActiveDirectoryUserRepository;
import com.cgr.base.domain.repository.IUserRepository;
import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class AuthService implements IAuthUseCase {

    private final IUserRepository userRepository;

    private final IUserRepositoryJpa userRepositoryFull;

    private final IActiveDirectoryUserRepository activeDirectoryUserRepository;

    private final JwtAuthenticationProvider jwtAuthenticationProvider;

    private final ILogUseCase logService;

    // Autenticación utilizando SAMAccountName y contraseña.
    @Transactional
    @Override
    public Map<String, Object> signIn(AuthRequestDto userRequest, HttpServletRequest servletRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        try {

            UserModel userModel = userRepository.findBySAMAccountName(userRequest.getSAMAccountName());

            System.err.println("userModel: " + userModel);
            if (userModel != null && userModel.getPassword().equals(userRequest.getPassword())) {

                Optional<UserEntity> userOptional = this.userRepositoryFull
                        .findBySAMAccountName(userRequest.getSAMAccountName());
                AuthResponseDto userDto = AuthMapper.INSTANCE.toAuthResponDto(userModel);

                String token = jwtAuthenticationProvider.createToken(userDto, userOptional.get().getRoles());

                userDto.setToken(token);
                userDto.setIsEnable(true);

                response.put("user", userDto);
                response.put("message", "User Authenticated Successfully.");
                response.put("statusCode", 200);
                response.put("status", "success");
                return response;

            }

        } catch (Exception e) {
            response.put("errormsj", e.getMessage());
            response.put("message", "Error Authenticating User.");
            response.put("statusCode", 500);
            response.put("status", "error");
        }
        return response;

    }

    // Autenticación en el Active Directory mediante LDAP
    @Transactional
    @Override
    public Map<String, Object> authWithLDAPActiveDirectory(AuthRequestDto userRequest,
            HttpServletRequest servletRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        try {

            Boolean isAccountValid = activeDirectoryUserRepository.checkAccount(
                    userRequest.getSAMAccountName(),
                    userRequest.getPassword());

            if (isAccountValid) {

                UserEntity user = this.userRepositoryFull
                        .findBySAMAccountNameWithRoles(userRequest.getSAMAccountName()).get();
                AuthResponseDto userRequestDto = AuthMapper.INSTANCE.toAuthResponDto(userRequest);

                List<AuthResponseDto.RoleDto> rolesDto = user.getRoles().stream()
                        .map(role -> new AuthResponseDto.RoleDto(role.getId(), role.getName()))
                        .toList();

                userRequestDto.setRoles(rolesDto);
                System.out.println("Roles asignados al usuario: " + rolesDto);

                String token = jwtAuthenticationProvider.createToken(userRequestDto, user.getRoles());

                List<Menu> menus = this.userRepositoryFull
                        .findMenusByRoleNames(user.getRoles().stream().map(RoleEntity::getName).toList());

                userRequestDto.setMenus(menus);

                userRequestDto.setToken(token);
                userRequestDto.setIsEnable(true);

                userRequest.setEmail(user.getEmail());
                this.logService.createLog(userRequest);

                response.put("user", userRequestDto);
                response.put("message", "User Authenticated Successfully.");
                response.put("statusCode", 200);
                response.put("status", "success");
                return response;
            }

        } catch (Exception e) {
            response.put("errormsj", e.getMessage());
            response.put("message", "Error Authenticating User with LDAP.");
            response.put("statusCode", 500);
            response.put("status", "error");
        }
        return response;
    }

}
