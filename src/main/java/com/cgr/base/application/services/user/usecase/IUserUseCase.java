package com.cgr.base.application.services.user.usecase;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesRequestDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesResponseDto;
import com.cgr.base.domain.models.entity.Logs.UserEntity;

public interface IUserUseCase {

    public List<UserWithRolesResponseDto> findAll();

    public UserWithRolesResponseDto assignRolesToUser(UserWithRolesRequestDto requestDto);

    public UserDto createUser(UserDto userRequestDto);

    public UserDto updateUser(Long id, UserDto userDto);

    public UserEntity uploadProfileImage(Long userId, String base64Image);

    public abstract String getProfileImage(Long userId);

    public abstract String extractImageType(String base64Image);

    public abstract String convertToBase64(MultipartFile image) throws IOException;

    Optional<UserWithRolesResponseDto> findById(Long id);
}
