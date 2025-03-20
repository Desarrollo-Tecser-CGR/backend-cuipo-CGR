package com.cgr.base.presentation.controller;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.role.dto.RoleRequestDto;
import com.cgr.base.application.role.usecase.IRoleService;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;

import jakarta.validation.Valid;

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
    public ResponseEntity<?> create(@Valid @RequestBody RoleEntity role, BindingResult result) {
        return requestResponse(result, () -> this.roleService.create(role), "Role Created.", HttpStatus.CREATED, true);
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

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable Long id) {
        RoleRequestDto role = this.roleService.activateOrDeactivate(id);
        return requestResponse(role, role.isEnable() ? "Role Activated." : "Role Deactivated.", HttpStatus.OK, true);
    }

}
