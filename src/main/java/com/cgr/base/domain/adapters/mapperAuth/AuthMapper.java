package com.cgr.base.domain.adapters.mapperAuth;

import java.util.function.Function;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.dto.dtoAuth.AuthResponseDto;
import com.cgr.base.domain.models.UserModel;

public enum AuthMapper implements Function<UserModel, AuthRequestDto> {
    INSTANCE;

    @Override
    public AuthRequestDto apply(UserModel t) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'apply'");
    }

    public AuthResponseDto toAuthResponDto(AuthRequestDto userRequestDto) {
        return mapToAuthResponseDto(userRequestDto.getUsername());
    }

    public AuthResponseDto toAuthResponDto(UserModel userEntity) {
        return mapToAuthResponseDto(userEntity.getSAMAccountName());
    }

    // Método privado común
    private AuthResponseDto mapToAuthResponseDto(String sAMAccountName) {
        return AuthResponseDto.builder()
                .isEnable(true)
                .build();
    }

}
