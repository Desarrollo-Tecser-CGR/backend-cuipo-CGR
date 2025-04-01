package com.cgr.base.application.services.role.service.permission.controllerPermission;

import com.cgr.base.application.services.role.service.permission.EntityPermission;
import com.cgr.base.application.services.role.service.permission.servicesPemission.ServicesPermission;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RequestMapping("/api/v1/permission")
@RestController
public class ControllerPermission {

    @Autowired
    ServicesPermission permission;

    @GetMapping ("/view")
    public ResponseEntity getPermission (){
        return permission.getPermission();
    }


}
