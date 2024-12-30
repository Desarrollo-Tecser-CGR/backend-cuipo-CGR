package com.test.testactivedirectory.application.auth.service;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.test.testactivedirectory.application.Email.EmailService;
import com.test.testactivedirectory.application.auth.dto.AuthRequestDto;
import com.test.testactivedirectory.application.auth.dto.AuthResponseDto;
import com.test.testactivedirectory.application.auth.dto.UserDto;
import com.test.testactivedirectory.application.auth.mapper.AuthMapper;
import com.test.testactivedirectory.application.auth.usecase.IAuthUseCase;
import com.test.testactivedirectory.domain.models.UserModel;
import com.test.testactivedirectory.domain.repository.IActiveDirectoryUserRepository;
import com.test.testactivedirectory.domain.repository.IUserRepository;
import com.test.testactivedirectory.infrastructure.exception.customException.ResourceNotFoundException;
import com.test.testactivedirectory.infrastructure.persistence.entity.UserEntity;
import com.test.testactivedirectory.infrastructure.persistence.repository.user.UserRepositoryJpa;
import com.test.testactivedirectory.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.test.testactivedirectory.infrastructure.utilities.EmailUtility;
import com.test.testactivedirectory.application.logs.usecase.LogUseCase;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class AuthService implements IAuthUseCase {

    private final IUserRepository userRepository;

    private final EmailService emailService;

    private final IActiveDirectoryUserRepository activeDirectoryUserRepository;

    private final JwtAuthenticationProvider jwtAuthenticationProvider;

    private final LogUseCase logService;

    private final UserRepositoryJpa userRepositoryJpa;

    @Override
    public Map<String, Object> signIn(AuthRequestDto userRequest, HttpServletRequest servletRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        try {

            UserModel userModel = userRepository.findBySAMAccountName(userRequest.getSAMAccountName());

            System.err.println("userModel: " + userModel);
            if (userModel != null && userModel.getPassword().equals(userRequest.getPassword())) {

                AuthResponseDto userDto = AuthMapper.INSTANCE.toAuthResponDto(userModel);

                String token = jwtAuthenticationProvider.createToken(userDto);

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

                this.logService.createLog(userRequest.getSAMAccountName());
                AuthResponseDto userRequestDto = AuthMapper.INSTANCE.toAuthResponDto(userRequest);

                String token = jwtAuthenticationProvider.createToken(userRequestDto);

                userRequestDto.setToken(token);
                userRequestDto.setIsEnable(true);

                response.put("user", userRequestDto);
                response.put("message", "User authenticated successfully");
                response.put("statusCode", 200);
                response.put("status", "success");
                return response;
            }

        } catch (Exception e) {
            // TODO: handle exception
            System.err.println("Error en la capa de aplicaciontion en service: " + e.getMessage());
        }
        response.put("message", "User not authenticated");
        return response;
    }


    @Transactional
    @Override
    public Map<String, Object> emailLogin(UserDto userRequest)
            throws JsonProcessingException {

        Map<String, Object> response = new HashMap<>();

        UserEntity userLogin = this.userRepositoryJpa.findBySAMAccountName(userRequest.getUser())
                .orElseThrow(() -> new ResourceNotFoundException("El usuario " + userRequest.getUser() + " no existe"));


        try {
            
            AuthResponseDto userToken = new AuthResponseDto ();
            userToken.setIsEnable(true);
            userToken.setSAMAccountName(userRequest.getUser());
            String emailToken = jwtAuthenticationProvider.createToken(userToken);

            this.emailService.sendSimpleEmail(userLogin.getEmail(), "Verificacion de Usuario", EmailUtility.getHtmlContent(emailToken));


        } catch (Exception e) {
            // TODO: handle exception
            System.err.println("Error en la capa de aplicaciontion en service: " + e.getMessage());
        }
        response.put("message", "User not authenticated");
        return response;
    }

}