package com.cgr.base.repository.user;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.entity.user.ProfileEntity;

public interface ProfileRepo extends JpaRepository<ProfileEntity, Long>
{
    
}
