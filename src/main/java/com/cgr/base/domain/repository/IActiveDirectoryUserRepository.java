package com.cgr.base.domain.repository;

import java.util.List;

import com.cgr.base.entity.user.UserEntity;

public interface IActiveDirectoryUserRepository {
    
    Boolean checkAccount(String samAccountName, String password);
    List<UserEntity> getAllUsers();
}
