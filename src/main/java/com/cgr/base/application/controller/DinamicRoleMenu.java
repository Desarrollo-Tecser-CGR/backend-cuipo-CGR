package com.cgr.base.application.controller;

import com.cgr.base.application.services.rolemenu.ServiceRoleMenu;
import com.cgr.base.domain.dto.dtoRoleMenu.DtoRoleMenu;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;



@RequestMapping("api/v1/dimanic_role_menu")
@RestController
public class DinamicRoleMenu {
    @Autowired
    private ServiceRoleMenu serviceRoleMenu;

    @GetMapping("/role/{roleId}")
    public ResponseEntity<List<DtoRoleMenu>> getRoleMenuByRoleId(@PathVariable Integer roleId) {
        List<DtoRoleMenu> roleMenus = serviceRoleMenu.getRoleMenuByRoleId(roleId);
        return new ResponseEntity<>(roleMenus, HttpStatus.OK);
    }

    @GetMapping("/all")
    public ResponseEntity<List<DtoRoleMenu>> getAllRoleMenus() {
        List<DtoRoleMenu> allRoleMenus = serviceRoleMenu.getAllRoleMenus();
        return new ResponseEntity<>(allRoleMenus, HttpStatus.OK);
    }
}
