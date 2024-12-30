package com.test.testactivedirectory.presentation.controller;

import org.apache.coyote.Response;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.test.testactivedirectory.application.role.usecase.RoleService;
import com.test.testactivedirectory.application.ventas.ventasService;

@RestController
@RequestMapping("/ventas/productos")
public class ventasController extends AbstractController  {
    private ventasService ventasService;
     public ventasController(ventasService ventasService) {
        this.ventasService = ventasService;
    }

    @GetMapping("/listar")
    public ResponseEntity <?> getAllVentas(){
        return ResponseEntity.ok(ventasService.getalList());
    }
}
