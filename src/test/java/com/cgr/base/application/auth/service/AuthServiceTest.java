package com.cgr.base.application.auth.service;

import com.cgr.base.application.Email.EmailService;
import com.cgr.base.application.auth.dto.AuthRequestDto;
import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.application.auth.dto.UserAuthDto;
import com.cgr.base.application.exception.customException.ResourceNotFoundException;
import com.cgr.base.application.logs.usecase.ILogUseCase;
import com.cgr.base.application.user.dto.UserDto;
import com.cgr.base.domain.models.UserModel;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Menu.Menu;
import com.cgr.base.domain.repository.IActiveDirectoryUserRepository;
import com.cgr.base.domain.repository.IUserRepository;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.cgr.base.infrastructure.utilities.DtoMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AuthServiceTest {

    @Mock
    private IUserRepository userRepository;
    @Mock
    private IUserRepositoryJpa userRepositoryFull;
    @Mock
    private IActiveDirectoryUserRepository activeDirectoryUserRepository;
    @Mock
    private JwtAuthenticationProvider jwtAuthenticationProvider;
    @Mock
    private EmailService emailService;
    @Mock
    private ILogUseCase logService;
    @Mock
    private DtoMapper dtoMapper;
    @Mock
    private HttpServletRequest servletRequest;

    @InjectMocks
    private AuthService authService;

    private AuthRequestDto authRequestDto;
    private UserAuthDto userAuthDto;
    private UserModel userModel;
    private UserEntity userEntity;
    private AuthResponseDto authResponseDto;
    private UserDto userDto;
    private RoleEntity roleEntity;
    private Menu menu;

    @BeforeEach
    void setUp() {
        authRequestDto = new AuthRequestDto();
        authRequestDto.setSAMAccountName("testUser");
        authRequestDto.setPassword("password");

        userAuthDto = new UserAuthDto();
        userAuthDto.setSAMAccountName("testUser");

        userModel = new UserModel();
        userModel.setSAMAccountName("testUser");
        userModel.setPassword("password");

        userEntity = new UserEntity();
        userEntity.setSAMAccountName("testUser");
        userEntity.setEnabled(true);
        userEntity.setUserType("externo");
        userEntity.setEmail("test@example.com");
        roleEntity = new RoleEntity();
        roleEntity.setName("admin");
        userEntity.setRoles(List.of(roleEntity));

        authResponseDto = new AuthResponseDto();
        authResponseDto.setIsEnable(true);
        authResponseDto.setRoles(List.of(roleEntity.getName()));

        // Configurar los campos correctos de la clase Menu
        menu = new Menu();
        menu.setTitle("testTitle");
        menu.setSubtitle("testSubtitle");
        menu.setType("testType");
        menu.setIcon("testIcon");

        authResponseDto.setMenus(List.of(menu));

        userDto = new UserDto();
        userDto.setEmail("test@example.com");
    }

    @Test
    void signIn_success() throws JsonProcessingException {
        when(userRepository.findBySAMAccountName(authRequestDto.getSAMAccountName())).thenReturn(userModel);
        when(userRepositoryFull.findBySAMAccountName(authRequestDto.getSAMAccountName())).thenReturn(Optional.of(userEntity));
        when(jwtAuthenticationProvider.createToken(any(AuthResponseDto.class), anyList(), anyInt())).thenReturn("testToken");

        Map<String, Object> result = authService.signIn(authRequestDto, servletRequest);

        assertNotNull(result);
        assertEquals("User authenticated successfully", result.get("message"));
        assertEquals(200, result.get("statusCode"));
        assertEquals("success", result.get("status"));
    }

    @Test
    void authWithLDAPActiveDirectory_success() throws JsonProcessingException {
        when(activeDirectoryUserRepository.checkAccount(authRequestDto.getSAMAccountName(), authRequestDto.getPassword())).thenReturn(true);
        when(userRepositoryFull.findBySAMAccountNameWithRoles(authRequestDto.getSAMAccountName())).thenReturn(Optional.of(userEntity));
        when(dtoMapper.convertToDto(userEntity, UserDto.class)).thenReturn(userDto);
        when(jwtAuthenticationProvider.createToken(any(AuthResponseDto.class), anyList(), anyInt())).thenReturn("testToken");
        when(userRepositoryFull.findMenusByRoleNames(anyList())).thenReturn(List.of(menu));

        Map<String, Object> result = authService.authWithLDAPActiveDirectory(authRequestDto, servletRequest);

        assertNotNull(result);
        assertEquals("User authenticated successfully", result.get("message"));
        assertEquals(200, result.get("statusCode"));
        assertEquals("success", result.get("status"));
    }

    @Test
    void authWithLDAPActiveDirectory_userNotFound() throws JsonProcessingException {
        when(activeDirectoryUserRepository.checkAccount(authRequestDto.getSAMAccountName(), authRequestDto.getPassword())).thenReturn(true);
        when(userRepositoryFull.findBySAMAccountNameWithRoles(authRequestDto.getSAMAccountName())).thenReturn(Optional.empty());

        assertThrows(ResourceNotFoundException.class, () -> authService.authWithLDAPActiveDirectory(authRequestDto, servletRequest));
    }

    @Test
    void emailLogin_success() throws JsonProcessingException {
        when(userRepositoryFull.findBySAMAccountNameWithRoles(userAuthDto.getSAMAccountName())).thenReturn(Optional.of(userEntity));
        when(dtoMapper.convertToDto(userEntity, UserDto.class)).thenReturn(userDto);
        when(jwtAuthenticationProvider.createToken(any(AuthResponseDto.class), anyList(), anyInt())).thenReturn("testToken");

        Map<String, Object> result = authService.emailLogin(userAuthDto);

        assertNotNull(result);
        assertEquals("Mail sent", result.get("message"));
    }
}