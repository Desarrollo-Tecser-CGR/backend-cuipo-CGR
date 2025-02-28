package com.cgr.base.infrastructure.repositories.repositories.auth;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.domain.models.entity.Logs.UserEntity;

@Repository
public interface IAuthRepositoryJpa extends JpaRepository<UserEntity,Long> { 

    // List<User> findBySAMAccountName(String sAMAccountName);
    UserEntity findBysAMAccountName(String sAMAccountName);

    
} 
