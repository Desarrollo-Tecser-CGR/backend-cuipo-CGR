package com.cgr.base.mapper.auth;

import java.util.function.Function;

import com.cgr.base.domain.models.UserModel;
import com.cgr.base.dto.auth.AuthRequestDto;
import com.cgr.base.entity.user.UserEntity;

public enum UserMapper implements Function<UserEntity, AuthRequestDto> {
    INSTANCE;

    @Override
    public AuthRequestDto apply(UserEntity userEntity) {

        if (userEntity != null) {
            AuthRequestDto userRequestDto = new AuthRequestDto();
            userRequestDto.setSAMAccountName(userEntity.getSAMAccountName());
            return userRequestDto;
        }
        return null;
    }

    public UserModel toUserEntity(UserEntity userREntity) {
        UserModel userModel = new UserModel();

        userModel.setId(userREntity.getId());
        userModel.setSAMAccountName(userREntity.getSAMAccountName());

        return userModel;
    }

    public AuthRequestDto toUserRequestDto(UserModel userModel) {
        AuthRequestDto userRequestDto = new AuthRequestDto();
        userRequestDto.setSAMAccountName(userModel.getSAMAccountName());
        userRequestDto.setPassword(userModel.getPassword());
        return userRequestDto;
    }
}
