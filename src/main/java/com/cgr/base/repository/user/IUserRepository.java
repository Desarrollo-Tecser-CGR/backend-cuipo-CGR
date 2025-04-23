package com.cgr.base.repository.user;

import com.cgr.base.entity.user.UserModel;

public interface IUserRepository {
     
    UserModel findBySAMAccountName(String sAMAccountName);

}
