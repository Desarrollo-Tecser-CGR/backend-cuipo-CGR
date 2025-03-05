package com.cgr.base.application.services.auth.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.services.Email.EmailService;
import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoAuth.AuthResponseDto;
import com.cgr.base.domain.dto.dtoAuth.UserAuthDto;
import com.cgr.base.domain.adapters.mapperAuth.AuthMapper;
import com.cgr.base.application.services.auth.usecase.IAuthUseCase;
import com.cgr.base.application.services.logs.usecase.ILogUseCase;
import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.models.UserModel;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.IActiveDirectoryUserRepository;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.IUserRepository;
import com.cgr.base.application.exception.customException.ResourceNotFoundException;
import com.cgr.base.domain.models.entity.Menu.Menu;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.cgr.base.infrastructure.utilities.DtoMapper;
import com.cgr.base.infrastructure.utilities.EmailUtility;
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

    private final EmailService emailService;

    private final ILogUseCase logService;

    private final DtoMapper dtoMapper;

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

                String token = jwtAuthenticationProvider.createToken(userDto, userOptional.get().getRoles(), 3600000);

                userDto.setToken(token);
                userDto.setIsEnable(true);

                response.put("user", userDto);
                response.put("message", "User authenticated successfully");
                response.put("statusCode", 200);
                response.put("status", "success");
                return response;

            }

        } catch (Exception e) {
            // TODO: handle exception
        }
        return null;

    }

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

                UserEntity user = this.userRepositoryFull.findBySAMAccountNameWithRoles(userRequest.getSAMAccountName())
                        .orElseThrow(() -> new ResourceNotFoundException(
                                "El usuario " + userRequest.getSAMAccountName() + " no existe"));

                if (user.getEnabled() == true) {
                    AuthResponseDto userRequestDto = new AuthResponseDto();

                    UserDto userDto = this.dtoMapper.convertToDto(user, UserDto.class);

                    // Asignar IDs de roles
                    userDto.setRoleIds(user.getRoles().stream()
                            .map(RoleEntity::getId)
                            .toList());

                    userRequestDto.setUser(userDto);

                    userRequestDto.setIsEnable(true);

                    userRequestDto.setRoles(user.getRoles().stream().map(RoleEntity::getName).toList());

                    String token = jwtAuthenticationProvider.createToken(userRequestDto, user.getRoles(), 3600000);

                    List<Menu> menus = this.userRepositoryFull
                            .findMenusByRoleNames(user.getRoles().stream().map(RoleEntity::getName).toList());

                    userRequestDto.setMenus(menus);

                    userRequestDto.setToken(token);

                    userRequest.setEmail(user.getEmail());
                    this.logService.createLog(userRequest);

                    response.put("user", userRequestDto);
                    response.put("message", "User authenticated successfully");
                    response.put("statusCode", 200);
                    response.put("status", "success");
                    return response;
                } else {
                    response.put("message", "User not enabled");
                    response.put("statusCode", 403);
                    response.put("status", "disabled");
                    return response;
                }

            }

        } catch (Exception e) {
            System.err.println("Error en la capa de aplicaciontion en service: " + e.getMessage());
        }
        response.put("message", "User not authenticated");
        return response;
    }

    @Transactional
    @Override
    public Map<String, Object> emailLogin(UserAuthDto userRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        UserEntity userLogin = this.userRepositoryFull.findBySAMAccountNameWithRoles(userRequest.getSAMAccountName())
                .orElseThrow(() -> new ResourceNotFoundException(
                        "El usuario " + userRequest.getSAMAccountName() + " no existe"));

        if ((userLogin.getEnabled() == true) && ("externo".equals(String.valueOf(userLogin.getUserType()).trim()))) {
            try {

                AuthResponseDto userToken = new AuthResponseDto();

                UserDto userDto = this.dtoMapper.convertToDto(userLogin, UserDto.class);

                userToken.setUser(userDto);

                userToken.setIsEnable(true);

                String emailToken = jwtAuthenticationProvider.createToken(userToken, userLogin.getRoles(), 300000);

                this.emailService.sendSimpleEmail(userDto.getEmail(), "Verificacion de Usuario",
                        EmailUtility.getHtmlContent(emailToken));

                response.put("message", "Mail sent");
                return response;

            } catch (Exception e) {
                // TODO: handle exception
                System.err.println("Error en la capa de aplicaciontion en service: " + e.getMessage());
            }
            response.put("message", "User not authenticated");
            return response;
        }

        else {
            response.put("message", "User not enabled");
            return response;
        }

    }

}
