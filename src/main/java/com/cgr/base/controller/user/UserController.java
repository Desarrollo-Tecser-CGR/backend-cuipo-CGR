package com.cgr.base.controller.user;

import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.dto.user.UserFilterRequestDto;
import com.cgr.base.dto.user.UserWithRolesRequestDto;
import com.cgr.base.service.user.IUserSynchronizerUseCase;
import com.cgr.base.service.user.IUserUseCase;

import jakarta.validation.Valid;

@PreAuthorize("hasAuthority('MENU_1')")
@RestController
@RequestMapping("/api/v1/user")
public class UserController extends AbstractController {

    private IUserUseCase userService;

    private IUserSynchronizerUseCase synchronizerUsers;

    public UserController(IUserUseCase userService, IUserSynchronizerUseCase synchronizerUsers) {
        this.userService = userService;
        this.synchronizerUsers = synchronizerUsers;
    }

    @GetMapping("/info/list")
    public ResponseEntity<?> getAll() {
        return requestResponse(this.userService.findAll(), "System Users.", HttpStatus.OK, true);
    }

    @PostMapping("/config/role")
    public ResponseEntity<?> assignRole(@Valid @RequestBody UserWithRolesRequestDto rolesRequestDto,
            BindingResult result) {
        return requestResponse(result, () -> this.userService.assignRolesToUser(rolesRequestDto), "Roles Updated.",
                HttpStatus.CREATED, true);
    }

    @GetMapping("/config/synchronize")
    public ResponseEntity<?> synchronizeAD() {
        return requestResponse(this.synchronizerUsers.synchronizeUsers(),
                "System Synchronized with Active Directory.", HttpStatus.OK, true);
    }

    @GetMapping("/info/filter")
    public ResponseEntity<?> findWithFilters(@Valid @RequestBody UserFilterRequestDto userFilter,
            BindingResult result, Pageable pageable) {
        return requestResponse(result, () -> this.userService.findWithFilters(userFilter, pageable),
                "List of Users with Filters.", HttpStatus.OK,
                true);
    }

}
