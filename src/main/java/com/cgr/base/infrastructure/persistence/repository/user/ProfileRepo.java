package com.cgr.base.infrastructure.persistence.repository.user;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.infrastructure.persistence.entity.user.ProfileEntity;

public interface ProfileRepo extends JpaRepository<ProfileEntity, Long>
{
    
}
