package com.cgr.base.repository.auth;

import com.cgr.base.entity.auth.UserBlockEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface IUserBlockRepo extends JpaRepository<UserBlockEntity, Long> {

    Optional<UserBlockEntity> findByUserId(Long userId);

}
