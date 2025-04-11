package com.cgr.base.application.user.mapper;

import com.cgr.base.application.user.dto.UserWithRolesResponseDto;
import com.cgr.base.entity.user.UserEntity;

public enum UserMapper {
    INSTANCE;

    public UserWithRolesResponseDto toUserWithRolesResponseDto(UserEntity userEntity) {
        
        var userDto = new UserWithRolesResponseDto();
        
        userDto.setIdUser(userEntity.getId());
        userDto.setUserName(userEntity.getSAMAccountName());
        userDto.setFullName(userEntity.getFullName());
        userDto.setEmail(userEntity.getEmail());
        userDto.setPhone(userEntity.getPhone());
        userDto.setEnabled(userEntity.getEnabled());
        userDto.setDateModify(userEntity.getDateModify());
        userDto.setCargo(userEntity.getCargo());
        userDto.addRole(userEntity.getRoles());
        
        return userDto;

    }
}
