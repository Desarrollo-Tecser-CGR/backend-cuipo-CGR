package com.cgr.base.application.controller.controllerNotification;

import com.cgr.base.domain.dto.dtoWebSocket.EntityNotificationDto;
import com.cgr.base.application.controller.AbstractController;
import com.cgr.base.application.services.servicesNotification.ServicesNotification;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/notification")
public class ControllerNotification extends AbstractController {

    @Autowired
    private ServicesNotification servicesNotification;

    @GetMapping
    public ResponseEntity<?> getAll() {

        List<EntityNotificationDto> notifications = this.servicesNotification.getAllNotifications();

        if (notifications == null) {
            return requestResponse("No hay información",
                    "Error en la consulta", HttpStatus.NOT_FOUND, true);
        }

        return requestResponse(notifications,
                "Contratos encontrados", HttpStatus.OK, true);
    }

    @GetMapping("/{entityId}")
    public ResponseEntity<?> getAllByEntity(@PathVariable Integer entityId) {

        List<EntityNotificationDto> notifications = this.servicesNotification.getNotificationsByEntity(entityId);

        if (notifications == null) {
            return requestResponse("No hay información",
                    "Error en la consulta", HttpStatus.NOT_FOUND, true);
        }

        return requestResponse(notifications,
                "Contratos encontrados", HttpStatus.OK, true);
    }

    // @PostMapping ("/saveNotification")
    // public ResponseEntity<EntityNotification> save ( @RequestBody
    // EntityNotification notification){
    // return servicesNotification.saveNotification(notification);
    // }

}
