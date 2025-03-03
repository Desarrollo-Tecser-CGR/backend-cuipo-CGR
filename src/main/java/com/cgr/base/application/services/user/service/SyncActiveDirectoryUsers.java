package com.cgr.base.application.services.user.service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.application.services.user.usecase.IUserSynchronizerUseCase;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.IActiveDirectoryUserRepository;
import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;

@Service
public class SyncActiveDirectoryUsers implements IUserSynchronizerUseCase {

    @Autowired
    private IActiveDirectoryUserRepository directoryUserRepository;

    @Autowired
    private IUserRepositoryJpa userRepositoryDB;

    @Transactional
    @Override
    public Boolean synchronizeUsers() {
        List<UserEntity> usersAD = this.directoryUserRepository.getAllUsers();
        try {
            usersAD.forEach(userAD -> {
                if (this.userRepositoryDB.existsBySAMAccountName(userAD.getSAMAccountName())) {

                    UserEntity userDB = this.userRepositoryDB.findBySAMAccountName(userAD.getSAMAccountName()).get();
                    if (!userDB.getDateModify().equals(userAD.getDateModify())) {
                        userDB.mapActiveDirectoryUser(userAD);
                        this.userRepositoryDB.save(userDB);
                    }

                } else {
                    this.userRepositoryDB.save(userAD);
                }
            });
        } catch (Exception ex) {
            return false;
        }

        return true;
    }

}
