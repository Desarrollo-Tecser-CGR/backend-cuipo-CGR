package com.cgr.base.service.user;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.external.LDAP.LDAPUsuarioRepository;
import com.cgr.base.repository.user.IUserRepositoryJpa;

import jakarta.transaction.Transactional;

@Service
public class UserAddService {

    @Autowired
    private LDAPUsuarioRepository ldapUserRepo;

    @Autowired
    private IUserRepositoryJpa userRepo;

    @Autowired
    private SyncActiveDirectoryUsers synchronizerUsers;

    @Transactional
    public UserEntity addUserIfNotExists(String samAccountName) {
        
        boolean userExists = ldapUserRepo.existsInExternalDirectory(samAccountName);
        if (!userExists) {
            throw new IllegalStateException("User does not exist in the external directory");
        }

        boolean userExistsInDB = userRepo.existsBySAMAccountName(samAccountName);
        if (userExistsInDB) {
            throw new IllegalStateException("User already exists in the system");
        }

        UserEntity newUser = new UserEntity();
        newUser.setSAMAccountName(samAccountName);
        userRepo.save(newUser);

        synchronizerUsers.synchronizeUsers();

        return userRepo.findBySAMAccountName(samAccountName)
                .orElseThrow(() -> new IllegalStateException("User not found in the database"));

    }

}
