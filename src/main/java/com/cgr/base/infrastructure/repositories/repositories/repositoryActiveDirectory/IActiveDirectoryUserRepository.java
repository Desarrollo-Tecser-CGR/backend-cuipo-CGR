package com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory;

import java.util.List;

import com.cgr.base.domain.models.entity.Logs.UserEntity;

public interface IActiveDirectoryUserRepository {

    Boolean checkAccount(String samAccountName, String password);

    List<UserEntity> getAllUsers();
}
