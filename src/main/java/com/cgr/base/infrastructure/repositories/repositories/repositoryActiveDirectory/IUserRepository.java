package com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory;

import com.cgr.base.domain.models.UserModel;

public interface IUserRepository {
     
    UserModel findBySAMAccountName(String sAMAccountName);
    
    // UserModel save(UserModel user);
    // UserModel update(UserModel user);
    // void delete(UserModel user);
    // List<UserModel> findAll();
}
