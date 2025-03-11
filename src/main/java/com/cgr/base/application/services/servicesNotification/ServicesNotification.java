package com.cgr.base.application.services.servicesNotification;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
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
