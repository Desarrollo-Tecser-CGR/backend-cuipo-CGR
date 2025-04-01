package com.cgr.base.application.controller;

import io.swagger.v3.oas.annotations.security.SecurityRequirement;

import java.io.IOException;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.dto.dtoUser.UserWithRolesRequestDto;
import com.cgr.base.application.services.user.usecase.IUserSynchronizerUseCase;
import com.cgr.base.application.services.user.usecase.IUserUseCase;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/user")
@SecurityRequirement(name = "BearerAuth")
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
        return requestResponse(result, () -> this.userService.assignRolesToUser(rolesRequestDto), "roles actualizados",
                HttpStatus.CREATED, true);
    }

    @GetMapping("/synchronize")
    public ResponseEntity<?> synchronizeAD() {
        return requestResponse(this.synchronizerUsers.synchronizeUsers(),
                "sistema sincronizado exitosamente con el Directorio Activo", HttpStatus.OK, true);
    }

    @PostMapping("/create")
    public ResponseEntity<?> createUser(@Valid @RequestBody UserDto userRequestDto,
            BindingResult result) {
        if (result.hasErrors()) {
            return requestResponse(result, "Error de validación en los datos proporcionados", HttpStatus.BAD_REQUEST,
                    false);
        }
        return requestResponse(result, () -> this.userService.createUser(userRequestDto), "Usuario creado exitosamente",
                HttpStatus.CREATED, true);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> updateUser(@PathVariable Long id, @Valid @RequestBody UserDto userDto,
            BindingResult result) {
        if (result.hasErrors()) {
            return requestResponse(result, "Error de validación en los datos proporcionados", HttpStatus.BAD_REQUEST,
                    false);
        }
        return requestResponse(result, () -> this.userService.updateUser(id, userDto),
                "Usuario actualizado exitosamente", HttpStatus.OK, true);

    }

    @PostMapping("/upload_image/{userId}")
    public ResponseEntity<String> uploadProfileImage(@PathVariable Long userId,
            @RequestParam("image") MultipartFile image,
            HttpServletRequest request) throws IOException {

        String base64Image = userService.convertToBase64(image);
        try {

            userService.uploadProfileImage(userId, base64Image);
            return new ResponseEntity<>("Profile Image Uploaded Successfully.", HttpStatus.OK);

        } catch (IllegalArgumentException e) {

            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/profile_image/{userId}")
    public ResponseEntity<String> getProfileImage(@PathVariable Long userId) {

        try {
            String base64Image = userService.getProfileImage(userId);
            return new ResponseEntity<>(base64Image, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.NOT_FOUND);
        }
    }

}
