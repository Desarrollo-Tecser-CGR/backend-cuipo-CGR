package com.cgr.base.service.user;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.repository.auth.IActiveDirectoryUserRepository;
import com.cgr.base.repository.user.IUserRepositoryJpa;

@Service
public class SyncActiveDirectoryUsers implements IUserSynchronizerUseCase {

    @Autowired
    private IActiveDirectoryUserRepository directoryUserRepository;

    @Autowired
    private IUserRepositoryJpa userRepositoryDB;

    // Sincroniza los usuarios del Active Directory en la BD.
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
