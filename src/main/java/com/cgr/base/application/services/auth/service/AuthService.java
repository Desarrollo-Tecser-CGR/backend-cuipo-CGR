package com.cgr.base.application.services.auth.service;

import java.time.LocalDateTime;
import java.util.*;

import com.cgr.base.domain.models.entity.Logs.LogEntity;
import org.apache.catalina.User;
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



    //jhon
    private static final int MAX_FAILED_ATTEMPTS = 3;
    private static final long LOCK_TIME_DURATION = 15; // Minutos

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

            }else {
                // Contraseña incorrecta, registrar intento fallido
                userRequest.setTipe_of_income("Fracaso");
                this.logService.createLog(userRequest);
                response.put("message", "Invalid username or password");
                response.put("statusCode", 401);
                response.put("status", "error");
                return response;
            }

        } catch (Exception e) {
            // TODO: handle exception
        }
        return null;

    }

    public Map<String, Object> authWithLDAPActiveDirectory(AuthRequestDto userRequest, HttpServletRequest servletRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        try {
            Boolean isAccountValid = activeDirectoryUserRepository.checkAccount(
                    userRequest.getSAMAccountName(), userRequest.getPassword());

            UserEntity user = this.userRepositoryFull.findBySAMAccountNameWithRoles(userRequest.getSAMAccountName())
                    .orElseThrow(() -> new ResourceNotFoundException(
                            "El usuario " + userRequest.getSAMAccountName() + " no existe"));

            if (isUserLocked(user)) {
                response.put("message", "Se han superado los 3 intentos fallidos de inicio de sesión. " +
                        "Tu cuenta ha sido bloqueada temporalmente por razones de seguridad. Por favor," +
                        " intenta nuevamente en 15 minutos o contacta con el soporte técnico si necesitas asistencia.");
                response.put("statusCode", 403);
                response.put("status", "error");
                return response;
            }

            if (isAccountValid) {
                if (user.getEnabled()) {
                    // Restablecer intentos fallidos si el inicio de sesión es exitoso
                    resetFailedAttempts(user);

                    userRequest.setTipe_of_income("Éxito");

                    AuthResponseDto userRequestDto = new AuthResponseDto();
                    UserDto userDto = this.dtoMapper.convertToDto(user, UserDto.class);
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
                    response.put("status", "error");
                    return response;
                }
            } else {
                // Incrementar intentos fallidos si la contraseña es incorrecta
                increaseFailedAttempts(user);

                userRequest.setTipe_of_income("Fracaso");

                Optional<UserEntity> userOptional = this.userRepositoryFull.findBySAMAccountName(userRequest.getSAMAccountName());
                String email = userOptional.map(UserEntity::getEmail).orElse("desconocido@dominio.com");
                userRequest.setEmail(email);

                this.logService.createLog(userRequest);

                response.put("message", "Invalid username or password");
                response.put("statusCode", 401);
                response.put("status", "error");
                return response;
            }

        } catch (Exception e) {
            System.err.println("Error en la capa de aplicación en service: " + e.getMessage());
        }

        response.put("message", "User not authenticated");
        return response;
    }

    // jhon
    private void increaseFailedAttempts(UserEntity user) {
        int failedAttempts = Objects.requireNonNullElse(user.getFailedAttempts(), 0);
        failedAttempts++;

        if (failedAttempts >= MAX_FAILED_ATTEMPTS) {
            user.setLockTime(LocalDateTime.now()); // Bloquear cuenta
            System.out.println("Usuario bloqueado por intentos fallidos: " + user.getSAMAccountName());
        }

        user.setFailedAttempts(failedAttempts);
        userRepositoryFull.save(user);
    }

    private boolean isUserLocked(UserEntity user) {
        if (user.getLockTime() == null) {
            return false;
        }

        LocalDateTime unlockTime = user.getLockTime().plusMinutes(LOCK_TIME_DURATION);
        if (LocalDateTime.now().isBefore(unlockTime)) {
            return true; // Aún bloqueado
        }

        // Si ya pasó el tiempo de bloqueo, restablece intentos fallidos y desbloquea
        resetFailedAttempts(user);
        return false;
    }

    private void resetFailedAttempts(UserEntity user) {
        user.setFailedAttempts(0);
        user.setLockTime(null);
        userRepositoryFull.save(user);
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