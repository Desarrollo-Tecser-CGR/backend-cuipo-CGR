package com.cgr.base.service.user;

import java.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.repository.auth.IActiveDirectoryUserRepository;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.cgr.base.external.LDAP.LDAPUsuarioRepository;

@Service
public class SyncActiveDirectoryUsers implements IUserSynchronizerUseCase {

    @Autowired
    private IActiveDirectoryUserRepository directoryUserRepository;

    @Autowired
    private IUserRepositoryJpa userRepositoryDB;

    @Autowired
    LDAPUsuarioRepository ldapUsuarioRepository;

    @Value("${external.auth.mode}")
    private String authMode;

    // Sincroniza los usuarios del Active Directory en la BD.
    @Transactional
    @Override
    public Boolean synchronizeUsers() {
        try {
            if ("ldap".equalsIgnoreCase(authMode)) {
                synchronizeLdapUsers();
            } else if ("cgr".equalsIgnoreCase(authMode)) {
                synchronizeCGRUsers();
            } else {
                throw new IllegalStateException("Unknown authentication mode: " + authMode);
            }
    
            return true;
        } catch (Exception ex) {
            // log.error("Error en sincronización de usuarios", ex);
            return false;
        }
    }

    private void synchronizeLdapUsers() {
        List<UserEntity> usersAD = directoryUserRepository.getAllUsers();
    
        usersAD.forEach(userAD -> {
            Optional<UserEntity> optionalUserDB = userRepositoryDB.findBySAMAccountName(userAD.getSAMAccountName());
    
            if (optionalUserDB.isPresent()) {
                UserEntity userDB = optionalUserDB.get();
                if (!userDB.getDateModify().equals(userAD.getDateModify())) {
                    userDB.mapActiveDirectoryUser(userAD);
                    userRepositoryDB.save(userDB);
                }
            } else {
                userRepositoryDB.save(userAD);
            }
        });
    }

    private void synchronizeCGRUsers() {
        List<UserEntity> usersInDB = userRepositoryDB.findAll();
    
        usersInDB.forEach(userDB -> {
            String samAccountName = userDB.getSAMAccountName();
            UserEntity userCGR = ldapUsuarioRepository.getUserDirectoryCGR(samAccountName);
    
            if (userCGR != null) {
                if (userCGR.getFullName() != null) {
                    userDB.setFullName(userCGR.getFullName());
                }
                if (userCGR.getEmail() != null) {
                    userDB.setEmail(userCGR.getEmail());
                }
                if (userCGR.getPhone() != null) {
                    userDB.setPhone(userCGR.getPhone());
                }
                userDB.setCargo(userCGR.getCargo());
    
            } else {

                userDB.setEnabled(false);
            }
    
            userRepositoryDB.save(userDB);
        });
    }
    
    
    



}
