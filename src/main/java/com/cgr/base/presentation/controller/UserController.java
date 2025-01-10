package com.cgr.base.presentation.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.user.dto.UserDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.application.user.usecase.IUserSynchronizerUseCase;
import com.cgr.base.application.user.usecase.IUserUseCase;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/user")
public class UserController extends AbstractController {

    private IUserUseCase userService;

    private IUserSynchronizerUseCase synchronizerUsers;

    public UserController(IUserUseCase userService, IUserSynchronizerUseCase synchronizerUsers) {
        this.userService = userService;
        this.synchronizerUsers = synchronizerUsers;
    }

    @GetMapping
    public ResponseEntity<?> getAll() {
        return requestResponse(this.userService.findAll(), "usuarios del sistema", HttpStatus.OK, true);
    }

    @PostMapping
    public ResponseEntity<?> assignRole(@Valid @RequestBody UserWithRolesRequestDto rolesRequestDto,
            BindingResult result) {
        return requestResponse(result, () -> this.userService.assignRolesToUser(rolesRequestDto), "roles actualizados", HttpStatus.CREATED, true);
    }

    @GetMapping("/synchronize")
    public ResponseEntity<?> synchronizeAD() {
        return requestResponse(this.synchronizerUsers.synchronizeUsers(),
                "sistema sincronizado exitosamente con el Directorio Activo", HttpStatus.OK, true);         
    }

    @PostMapping ("/create")
    public ResponseEntity<?> createUser(@Valid @RequestBody UserDto userRequestDto, 
            BindingResult result) {
                if (result.hasErrors()) {
        return requestResponse(result, "Error de validación en los datos proporcionados", HttpStatus.BAD_REQUEST, false);
            }
        return requestResponse(result,() -> this.userService.createUser(userRequestDto), "Usuario creado exitosamente", HttpStatus.CREATED, true);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateUser(@PathVariable Long id, @Valid @RequestBody UserDto userDto, 
            BindingResult result) {
                if (result.hasErrors()) {
        return requestResponse(result, "Error de validación en los datos proporcionados", HttpStatus.BAD_REQUEST, false);
            }
        return requestResponse(result,() -> this.userService.updateUser(id, userDto), "Usuario actualizado exitosamente", HttpStatus.OK, true);
                
    }

}
