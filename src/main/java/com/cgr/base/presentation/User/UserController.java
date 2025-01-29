package com.cgr.base.presentation.User;

import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.user.dto.UserFilterRequestDto;
import com.cgr.base.application.user.dto.UserWithRolesRequestDto;
import com.cgr.base.application.user.usecase.IUserSynchronizerUseCase;
import com.cgr.base.application.user.usecase.IUserUseCase;
import com.cgr.base.presentation.controller.AbstractController;

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
        return requestResponse(this.userService.findAll(), "System Users.", HttpStatus.OK, true);
    }

    @PostMapping
    public ResponseEntity<?> assignRole(@Valid @RequestBody UserWithRolesRequestDto rolesRequestDto,
            BindingResult result) {
        return requestResponse(result, () -> this.userService.assignRolesToUser(rolesRequestDto), "Roles Updated.",
                HttpStatus.CREATED, true);
    }

    @GetMapping("/synchronize")
    public ResponseEntity<?> synchronizeAD() {
        return requestResponse(this.synchronizerUsers.synchronizeUsers(),
                "System Synchronized with Active Directory.", HttpStatus.OK, true);
    }

    @GetMapping("/filter")
    public ResponseEntity<?> findWithFilters(@Valid @RequestBody UserFilterRequestDto userFilter,
            BindingResult result, Pageable pageable) {
        return requestResponse(result, () -> this.userService.findWithFilters(userFilter, pageable),
                "List of Users with Filters.", HttpStatus.OK,
                true);
    }

}
