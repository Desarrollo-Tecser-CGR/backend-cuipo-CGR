package com.cgr.base.application.auth.mapper;

import java.util.function.Function;

import com.cgr.base.application.auth.dto.AuthRequestDto;
import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.domain.models.UserModel;

public enum AuthMapper implements Function<UserModel, AuthRequestDto> {
    INSTANCE;

    // Excepción de Método NO Implementado.
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
