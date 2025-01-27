package com.cgr.base.domain.repository;

import com.cgr.base.domain.models.UserModel;

public interface IUserRepository {
     
    UserModel findBySAMAccountName(String sAMAccountName);

}
