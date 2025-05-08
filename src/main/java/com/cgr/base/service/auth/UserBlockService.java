package com.cgr.base.service.auth;

import com.cgr.base.entity.auth.UserBlockEntity;
import com.cgr.base.repository.auth.IUserBlockRepo;
import com.cgr.base.repository.logs.ILogsRepositoryJpa;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@Service
public class UserBlockService {

    @Autowired
    private IUserBlockRepo userBlockRepository;

    @Autowired
    private ILogsRepositoryJpa logRepository;

    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");

    // Método para verificar si el usuario está bloqueado
    public boolean isUserBlocked(Long userId) {
        String fiveMinutesAgo = LocalDateTime.now().minusMinutes(5).format(formatter);

        int failedAttempts = logRepository.countFailedAttemptsInLast5Minutes(userId, fiveMinutesAgo);
        return failedAttempts >= 3;
    }

    // Método para bloquear al usuario
    public void blockUser(Long userId) {
        UserBlockEntity userBlock = new UserBlockEntity();
        userBlock.setUserId(userId);

        String blockedUntil = LocalDateTime.now().plusMinutes(15).format(formatter);
        userBlock.setBlockedUntil(blockedUntil);

        userBlockRepository.save(userBlock);
    }

    // Método para verificar y bloquear al usuario si es necesario
    public void checkAndBlockUser(Long userId) {

        String fiveMinutesAgo = LocalDateTime.now().minusMinutes(5).format(formatter);

        int failedAttempts = logRepository.countFailedAttemptsInLast5Minutes(userId, fiveMinutesAgo);

        if (failedAttempts >= 3) {
            blockUser(userId);
        }
    }
}
