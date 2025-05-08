package com.cgr.base.repository.auth;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.user.UserEntity;

@Repository
public interface IAuthRepositoryJpa extends JpaRepository<UserEntity,Long> { 

    // List<User> findBySAMAccountName(String sAMAccountName);
    UserEntity findBysAMAccountName(String sAMAccountName);

    
} 
