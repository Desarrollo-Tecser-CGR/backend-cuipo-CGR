package com.cgr.base.application.controller.controllerNotification;


import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.application.services.servicesNotification.ServicesNotification;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping ("/api/v1/notification")
public class ControllerNotification {

    @Autowired
    private ServicesNotification servicesNotification;

    @PostMapping ("/saveNotification")
    public ResponseEntity<EntityNotification> save ( @RequestBody EntityNotification notification){
        return servicesNotification.saveNotification(notification);
    }

}
