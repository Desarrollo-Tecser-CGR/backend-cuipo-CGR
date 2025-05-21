package com.cgr.base.controller.role;

import java.util.*;

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

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import static com.cgr.base.entity.logs.LogType.USUARIOS;
import com.cgr.base.entity.role.RoleEntity;
import com.cgr.base.service.logs.LogGeneralService;
import com.cgr.base.service.role.IRoleService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@PreAuthorize("hasAuthority('MENU_ACCESS')")
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
        // Validate required fields
        if (!roleData.containsKey("name")) {
            return requestResponse(null, "El nombre del rol es obligatorio.", HttpStatus.BAD_REQUEST, false);
        }
        if (!roleData.containsKey("description")) {
            return requestResponse(null, "La descripción es obligatoria.", HttpStatus.BAD_REQUEST, false);
        }

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

        logGeneralService.createLog(userId, USUARIOS,
                "Creación de Rol " + createdRole.getId() + " : " + createdRole.getName(), response);

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

        Map<String, Object> detalle = new LinkedHashMap<>();
        detalle.put("ID", id);
        detalle.put("Nombre", updatedRole.getName());
        detalle.put("Descripción", updatedRole.getDescription());

        logGeneralService.createLog(userId, USUARIOS,
                "Modificación de Rol con ID: " + id, detalle);

        return requestResponse(response, "Update operation completed.", HttpStatus.OK, true);
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

        logGeneralService.createLog(userId, USUARIOS,
                "Modificación de Estado de Activación Rol con ID: " + id, Map.of("Activo", isEnabled));

        return requestResponse(Map.of("message", message, "enabled", isEnabled), "Update operation completed.",
                HttpStatus.OK, true);

    }

    @DeleteMapping("/config/{id}")
    public ResponseEntity<?> deleteRole(@PathVariable Long id, HttpServletRequest request) {
        if (id == 1 || id == 2 || id == 3) {
            return requestResponse(null, "Los roles principales del sistema no pueden ser eliminados.",
                    HttpStatus.FORBIDDEN, false);
        }

        roleService.delete(id);

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        logGeneralService.createLog(userId, USUARIOS,
                "Eliminación de Rol con ID: " + id, null);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Rol eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }

}
