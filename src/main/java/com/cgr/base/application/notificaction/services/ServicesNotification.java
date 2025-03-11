package com.cgr.base.application.notificaction.services;

import com.cgr.base.application.notificaction.entity.EntityNotification;
import com.cgr.base.application.notificaction.repository.RepositoryNotification;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class ServicesNotification {

    @Autowired
     private RepositoryNotification repositoryNotification;


    public ResponseEntity <EntityNotification> saveNotification(EntityNotification notification) {
          EntityNotification saveNotificaction  = repositoryNotification.save(notification);
        return new ResponseEntity<>(saveNotificaction, HttpStatus.CREATED);
    }


}
