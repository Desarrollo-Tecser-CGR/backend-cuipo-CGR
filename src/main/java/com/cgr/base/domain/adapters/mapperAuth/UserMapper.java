package com.cgr.base.domain.adapters.mapperAuth;

import java.util.function.Function;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.models.UserModel;
import com.cgr.base.domain.models.entity.Logs.UserEntity;

public enum UserMapper implements Function<UserEntity, AuthRequestDto> {
    INSTANCE;

    @Override
    public AuthRequestDto apply(UserEntity userEntity) {

        if (userEntity != null) {
            AuthRequestDto userRequestDto = new AuthRequestDto();
            userRequestDto.setSAMAccountName(userEntity.getSAMAccountName());
            userRequestDto.setPassword(userEntity.getPassword());
            return userRequestDto;
        }
        return null;
    }

    public UserModel toUserEntity(UserEntity userREntity) {
        UserModel userModel = new UserModel();

        userModel.setId(userREntity.getId());
        userModel.setSAMAccountName(userREntity.getSAMAccountName());
        userModel.setPassword(userREntity.getPassword());

        return userModel;
    }

    public AuthRequestDto toUserRequestDto(UserModel userModel) {
        AuthRequestDto userRequestDto = new AuthRequestDto();
        userRequestDto.setSAMAccountName(userModel.getSAMAccountName());
        userRequestDto.setPassword(userModel.getPassword());
        return userRequestDto;
    }
}
