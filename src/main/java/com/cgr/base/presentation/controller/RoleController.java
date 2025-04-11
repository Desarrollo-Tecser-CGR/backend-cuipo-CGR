package com.cgr.base.presentation.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.logs.service.LogGeneralService;
import com.cgr.base.application.role.usecase.IRoleService;
import com.cgr.base.config.abstractResponse.AbstractController;
import static com.cgr.base.infrastructure.persistence.entity.log.LogType.USUARIOS;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@PreAuthorize("hasAuthority('MENU_1')")
@RequestMapping("/api/v1/role")
public class RoleController extends AbstractController {

    private IRoleService roleService;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private LogGeneralService logGeneralService;

    public RoleController(IRoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping("/info")
    public ResponseEntity<?> getAll() {
        return requestResponse(this.roleService.findAll(), "System Roles.", HttpStatus.OK, true);
    }

    @GetMapping("/info/{id}")
    public ResponseEntity<?> getById(@PathVariable Long id) {
        return requestResponse(this.roleService.findById(id), "Role Found.", HttpStatus.OK, true);
    }

    @PostMapping("/config")
    public ResponseEntity<?> createRole(@RequestBody Map<String, Object> roleData, HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        String name = (String) roleData.get("name");
        String description = (String) roleData.get("description");

        boolean enable = roleData.containsKey("enable") ? Boolean.parseBoolean(roleData.get("enable").toString())
                : true;

        RoleEntity newRole = new RoleEntity();
        newRole.setName(name);
        newRole.setDescription(description);
        newRole.setEnable(enable);

        RoleEntity createdRole = roleService.create(newRole);

        Map<String, Object> response = new HashMap<>();
        response.put("id", createdRole.getId());
        response.put("name", createdRole.getName());
        response.put("description", createdRole.getDescription());
        response.put("enable", createdRole.isEnable());

        try {

            logGeneralService.createLog(userId, USUARIOS,
                    "Creación de rol id : " + createdRole.getId() +" nombre: " + createdRole.getName() + "con descripción: " + createdRole.getDescription()
                            + " y estado: " + createdRole.isEnable() + ".");

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PutMapping("/config")
    public ResponseEntity<?> updateRole(@RequestBody Map<String, Object> roleData, HttpServletRequest request) {
        Long id = Long.valueOf(roleData.get("id").toString());
        String name = (String) roleData.get("name");
        String description = (String) roleData.get("description");

        RoleEntity updatedRole = roleService.update(id, name, description);

        Map<String, Object> response = new HashMap<>();
        response.put("id", updatedRole.getId());
        response.put("name", updatedRole.getName());
        response.put("description", updatedRole.getDescription());

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        try {

            logGeneralService.createLog(userId, USUARIOS,
                    "Modificación de rol id: " + id + " a: " + updatedRole.getName() + " con descripción: "
                            + updatedRole.getDescription() + ".");

            return requestResponse(response, "Update operation completed.", HttpStatus.OK, true);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }
    }

    @PutMapping("/config/{id}")
    public ResponseEntity<?> toggleRoleStatus(@PathVariable Long id, HttpServletRequest request) {
        boolean isEnabled = this.roleService.toggleStatus(id);
        String message = isEnabled ? "Role Activated." : "Role Deactivated.";

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        try {

            logGeneralService.createLog(userId, USUARIOS,
                    "Modificación de rol id: " + id + " a: " + message + ".");

            return requestResponse(Map.of("message", message, "enabled", isEnabled), "Update operation completed.",
                    HttpStatus.OK, true);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }

    }

    @DeleteMapping("/config/{id}")
    public ResponseEntity<?> deleteRole(@PathVariable Long id, HttpServletRequest request) {
        roleService.delete(id);

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        try {
            logGeneralService.createLog(userId, USUARIOS,
                    "Eliminación de rol id: " + id + ".");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Rol eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }

}
