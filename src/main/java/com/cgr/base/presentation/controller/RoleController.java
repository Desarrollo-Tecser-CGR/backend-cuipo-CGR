package com.cgr.base.presentation.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.role.usecase.IRoleService;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;

@RestController
@RequestMapping("/api/v1/role")
public class RoleController extends AbstractController {

    private IRoleService roleService;

    public RoleController(IRoleService roleService) {
        this.roleService = roleService;
    }

    @GetMapping
    public ResponseEntity<?> getAll() {
        return requestResponse(this.roleService.findAll(), "System Roles.", HttpStatus.OK, true);
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable Long id) {
        return requestResponse(this.roleService.findById(id), "Role Found.", HttpStatus.OK, true);
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> createRole(@RequestBody Map<String, Object> roleData) {
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

        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    @PutMapping
    public ResponseEntity<Map<String, Object>> updateRole(@RequestBody Map<String, Object> roleData) {
        Long id = Long.valueOf(roleData.get("id").toString());
        String name = (String) roleData.get("name");
        String description = (String) roleData.get("description");

        RoleEntity updatedRole = roleService.update(id, name, description);

        Map<String, Object> response = new HashMap<>();
        response.put("id", updatedRole.getId());
        response.put("name", updatedRole.getName());
        response.put("description", updatedRole.getDescription());

        return ResponseEntity.ok(response);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> toggleRoleStatus(@PathVariable Long id) {
        boolean isEnabled = this.roleService.toggleStatus(id);
        String message = isEnabled ? "Role Activated." : "Role Deactivated.";
        return ResponseEntity.ok().body(Map.of("message", message, "enabled", isEnabled));
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Map<String, Object>> deleteRole(@PathVariable Long id) {
        roleService.delete(id);

        Map<String, Object> response = new HashMap<>();
        response.put("message", "Rol eliminado exitosamente.");
        return ResponseEntity.ok(response);
    }

}
