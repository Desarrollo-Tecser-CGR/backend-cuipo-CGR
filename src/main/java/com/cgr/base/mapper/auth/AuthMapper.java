package com.cgr.base.mapper.auth;

import java.util.function.Function;

import com.cgr.base.domain.models.UserModel;
import com.cgr.base.dto.auth.AuthRequestDto;
import com.cgr.base.dto.auth.AuthResponseDto;

public enum AuthMapper implements Function<UserModel, AuthRequestDto> {
    INSTANCE;

    @Override
    public AuthRequestDto apply(UserModel t) {
        throw new UnsupportedOperationException("Unimplemented method 'apply'");
    }

    public AuthResponseDto toAuthResponDto(AuthRequestDto userRequestDto) {
        return mapToAuthResponseDto(userRequestDto.getSAMAccountName());
    }

    public AuthResponseDto toAuthResponDto(UserModel userEntity) {
        return mapToAuthResponseDto(userEntity.getSAMAccountName());
    }

    // Construir un objeto AuthResponseDto a partir del SAMAccountName.
    private AuthResponseDto mapToAuthResponseDto(String sAMAccountName) {
        return AuthResponseDto.builder()
                .sAMAccountName(sAMAccountName)
                .isEnable(true)
                .build();
    }

}
