package com.cgr.base.infrastructure.persistence.adapter;

import org.springframework.stereotype.Component;

import com.cgr.base.domain.models.UserModel;
import com.cgr.base.domain.repository.IUserRepository;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.infrastructure.persistence.repository.auth.IAuthRepositoryJpa;
import com.cgr.base.mapper.auth.UserMapper;

@Component
public class AuthRepositoryAdapterImpl implements IUserRepository {

  private final IAuthRepositoryJpa authRepositoryJpa;

  public AuthRepositoryAdapterImpl(IAuthRepositoryJpa authRepositoryJpa) {
    this.authRepositoryJpa = authRepositoryJpa;
  }

  // Buscar un usuario por SAMAccountName
  @Override
  public UserModel findBySAMAccountName(String sAMAccountName) {

    try {
      UserEntity userEntity = authRepositoryJpa.findBysAMAccountName(sAMAccountName);
      if (userEntity.hashCode() > 0) {
        return UserMapper.INSTANCE.toUserEntity(userEntity);
      }

    } catch (Exception e) {
      return null;
    }
    return null;
  }

}
