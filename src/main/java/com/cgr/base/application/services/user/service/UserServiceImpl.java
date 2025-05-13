package com.cgr.base.application.services.user.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.domain.dto.dtoLogs.logsIngress.LogDto;
import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesRequestDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesResponseDto;
import com.cgr.base.application.services.user.usecase.IUserUseCase;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.ILogRepository;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.IUserRoleRepository;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import com.cgr.base.domain.models.entity.Logs.LogEntity;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.utilities.DtoMapper;

import lombok.AllArgsConstructor;
import org.springframework.web.server.ResponseStatusException;

@Service
@AllArgsConstructor
public class UserServiceImpl implements IUserUseCase {

    private final IUserRoleRepository userRoleRepository;

    private final IUserRepositoryJpa userRepositoryJpa;

    private final ILogRepository logRepository;

    private final DtoMapper dtoMapper;

    private static final int MAX_IMAGE_SIZE = 10 * 1024 * 1024; // 2MB 10MB

    private static final String[] ALLOWED_FORMATS = { "image/png", "image/jpeg", "image/svg+xml" };

    @Transactional(readOnly = true)
    @Override
    public List<UserWithRolesResponseDto> findAll() {
        List<UserWithRolesResponseDto> users = new ArrayList<>();
        this.userRoleRepository.findAll().forEach(user -> {
            var userResponsive = new UserWithRolesResponseDto();
            userResponsive.setIdUser(user.getId());
            userResponsive.setUserName(user.getSAMAccountName());
            userResponsive.setFullName(user.getFullName());
            userResponsive.setEmail(user.getEmail());
            userResponsive.setPhone(user.getPhone());
            userResponsive.setEnabled(user.getEnabled());
            userResponsive.setDateModify(user.getDateModify());
            userResponsive.setCargo(user.getCargo());
            userResponsive.setUserType(user.getUserType());
            userResponsive.setImageProfile(user.getImageProfile());

            List<LogEntity> logs = this.logRepository.findLogByUserEntityId(user.getId());
            List<LogDto> logDtos = this.dtoMapper.convertToListDto(logs, LogDto.class);

            userResponsive.setLogs(logDtos);
            userResponsive.addRole(user.getRoles());

            users.add(userResponsive);
        });
        return users;
    }

    @Transactional
    public UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto) {
        // 1. Obtener la lista de usuarios con el rol de administrador
        List<UserEntity> adminUsers = userRoleRepository.findUsersByRoleName("administrador");

        if (adminUsers.size() >= 3 && requestDto.getRoles().contains("administrador")) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "No se pueden asignar más de tres administradores.");
        }

        UserEntity userEntity = this.userRoleRepository.assignRolesToUser(requestDto);

        var userResponsive = new UserWithRolesResponseDto();
        userResponsive.setIdUser(userEntity.getId());
        userResponsive.setUserName(userEntity.getSAMAccountName());
        userResponsive.setFullName(userEntity.getFullName());
        userResponsive.setEmail(userEntity.getEmail());
        userResponsive.setPhone(userEntity.getPhone());
        userResponsive.setEnabled(userEntity.getEnabled());
        userResponsive.setDateModify(userEntity.getDateModify());
        userResponsive.setCargo(userEntity.getCargo());
        userResponsive.addRole(userEntity.getRoles());

        return userResponsive;
    }

    @Transactional
    @Override
    public UserDto createUser(UserDto userRequestDto) {

        UserDto user = this.userRoleRepository.createUser(userRequestDto);

        if (user != null) {
            return user;
        }
        return null;

    }

    @Override
    public UserDto updateUser(Long id, UserDto userDto) {
        UserDto user = this.userRoleRepository.updateUser(id, userDto);

        if (user != null) {
            return user;
        }
        return null;
    }

    public UserEntity uploadProfileImage(Long userId, String base64Image) {

        byte[] decodedBytes = Base64.getDecoder().decode(base64Image.split(",")[1]);
        if (decodedBytes.length > MAX_IMAGE_SIZE) {
            throw new IllegalArgumentException("Image size exceeds the limit.");
        }

        String imageType = extractImageType(base64Image);

        boolean isValidFormat = false;
        for (String format : ALLOWED_FORMATS) {
            if (imageType.equals(format)) {
                isValidFormat = true;
                break;
            }
        }
        if (!isValidFormat) {
            throw new IllegalArgumentException("Invalid image format. Only PNG, JPG, and SVG are allowed.");
        }

        Optional<UserEntity> existingProfile = userRepositoryJpa.findById(userId);

        if (!existingProfile.isPresent()) {
            return null;
        }

        UserEntity user = existingProfile.get();
        user.setImageProfile(base64Image);

        return userRepositoryJpa.save(user);

    }

    // Método para obtener la imagen de perfil en base64
    public String getProfileImage(Long userId) {
        UserEntity profile = userRepositoryJpa.findById(userId).get();
        if (profile == null || profile.getImageProfile() == null) {
            return "imagen no encontrada";
        }
        return profile.getImageProfile();
    }

    // Extraer el tipo MIME de la cadena Base64.
    public String extractImageType(String base64Image) {
        String regex = "data:image/([a-zA-Z]*);base64,";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(base64Image);
        if (matcher.find()) {
            return "image/" + matcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid Base64 image format.");
        }
    }

    public String convertToBase64(MultipartFile image) throws IOException {
        byte[] bytes = image.getBytes();
        return "data:" + image.getContentType() + ";base64," + Base64.getEncoder().encodeToString(bytes);
    }

    @Override
    public Optional<UserWithRolesResponseDto> findById(Long id) {
        Optional<UserEntity> userEntityOptional = userRepositoryJpa.findById(id);
        if (userEntityOptional.isPresent()) {
            UserEntity userEntity = userEntityOptional.get();
            UserWithRolesResponseDto userResponse = new UserWithRolesResponseDto();
            userResponse.setIdUser(userEntity.getId());
            userResponse.setUserName(userEntity.getSAMAccountName());
            userResponse.setFullName(userEntity.getFullName());
            userResponse.setEmail(userEntity.getEmail());
            userResponse.setPhone(userEntity.getPhone());
            userResponse.setEnabled(userEntity.getEnabled());
            userResponse.setDateModify(userEntity.getDateModify());
            userResponse.setCargo(userEntity.getCargo());
            userResponse.setUserType(userEntity.getUserType());
            userResponse.setImageProfile(userEntity.getImageProfile());
            userResponse.addRole(userEntity.getRoles());
            return Optional.of(userResponse);
        }
        return Optional.empty();
    }

}