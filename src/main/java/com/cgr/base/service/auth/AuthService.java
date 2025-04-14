package com.cgr.base.service.auth;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.common.exception.exceptionCustom.ResourceNotFoundException;
import com.cgr.base.dto.auth.AuthRequestDto;
import com.cgr.base.dto.auth.AuthResponseDto;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.entity.log.LogEntity;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;
import com.cgr.base.mapper.auth.AuthMapper;
import com.cgr.base.repository.auth.IActiveDirectoryUserRepository;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.cgr.base.service.logs.LogService;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.servlet.http.HttpServletRequest;
import lombok.AllArgsConstructor;

@Service
@AllArgsConstructor
public class AuthService {

    @Autowired
    private final LogService LogService;

    @Autowired
    private final IUserRepositoryJpa userRepositoryFull;

    @Autowired
    private final IActiveDirectoryUserRepository activeDirectoryUserRepository;

    @Autowired
    private final JwtAuthenticationProvider jwtAuthenticationProvider;

    @Autowired
    private final JwtService jwtService;

    // Autenticación en el Active Directory
    public Map<String, Object> authWithLDAPActiveDirectory(AuthRequestDto userRequest,
            HttpServletRequest servletRequest) throws JsonProcessingException {

        if (userRequest.getSAMAccountName() == null || userRequest.getSAMAccountName().isBlank()) {
            throw new IllegalArgumentException("SAMAccountName is required");
        }

        if (userRequest.getPassword() == null || userRequest.getPassword().isBlank()) {
            throw new IllegalArgumentException("Password is required");
        }

        Optional<UserEntity> userOpt = userRepositoryFull
                .findBySAMAccountNameWithRoles(userRequest.getSAMAccountName());
        if (userOpt.isEmpty()) {
            throw new ResourceNotFoundException("User not found");
        }

        UserEntity user = userOpt.get();

        boolean isAccountValid = activeDirectoryUserRepository.checkAccount(
                userRequest.getSAMAccountName(),
                userRequest.getPassword());

        if (!isAccountValid) {
            throw new SecurityException("Invalid credentials");
        }

        AuthResponseDto userRequestDto = AuthMapper.INSTANCE.toAuthResponDto(userRequest);

        List<AuthResponseDto.RoleDto> rolesDto = user.getRoles().stream()
                .map(role -> new AuthResponseDto.RoleDto(role.getId(), role.getName()))
                .toList();
        userRequestDto.setRoles(rolesDto);

        String token = jwtAuthenticationProvider.createToken(userRequestDto, user.getRoles());
        userRequestDto.setToken(token);
        userRequestDto.setIsEnable(true);

        List<Menu> menus = userRepositoryFull.findMenusByRoleNames(
                user.getRoles().stream().map(RoleEntity::getName).toList());
        userRequestDto.setMenus(menus);

        userRequest.setEmail(user.getEmail());

        Long userId = jwtService.extractUserIdFromToken(token);
        LogEntity log = new LogEntity();
        log.setDateSessionStart(LocalDateTime.now()
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("America/Bogota"))));
        log.setUserId(userId);
        log.setRoles(user.getRoles().stream().map(RoleEntity::getName).collect(Collectors.joining(",")));
        LogService.saveLog(log);

        return Map.of("user", userRequestDto);
    }

}
