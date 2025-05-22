package com.cgr.base.controller.user;

import java.util.stream.Collectors;
import java.util.*;

import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.dto.user.UserFilterRequestDto;
import com.cgr.base.dto.user.UserWithRolesRequestDto;
import com.cgr.base.dto.user.UserWithRolesResponseDto;
import com.cgr.base.entity.role.RoleEntity;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.external.LDAP.LDAPUsuarioRepository;
import com.cgr.base.repository.role.IRoleRepository;
import com.cgr.base.service.user.IUserSynchronizerUseCase;
import com.cgr.base.service.user.IUserUseCase;
import com.cgr.base.service.user.UserAddService;

import static com.cgr.base.entity.logs.LogType.USUARIOS;
import com.cgr.base.service.logs.LogGeneralService;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;

@PreAuthorize("hasAuthority('MENU_ACCESS')")
@RestController
@RequestMapping("/api/v1/user")
public class UserController extends AbstractController {

    private final IUserUseCase userService;
    private final IUserSynchronizerUseCase synchronizerUsers;
    private final JwtService jwtService;
    private final IRoleRepository roleRepository;
    private final LogGeneralService logGeneralService;
    private final UserAddService userAddService;

    public UserController(IUserUseCase userService, IUserSynchronizerUseCase synchronizerUsers, JwtService jwtService,
            IRoleRepository roleRepository, LogGeneralService logGeneralService,
            UserAddService userAddService) {
        this.userService = userService;
        this.synchronizerUsers = synchronizerUsers;
        this.jwtService = jwtService;
        this.roleRepository = roleRepository;
        this.logGeneralService = logGeneralService;
        this.userAddService = userAddService;
    }

    @GetMapping("/info/list")
    public ResponseEntity<?> getAll(HttpServletRequest request) {
        return requestResponse(this.userService.findAll(), "System Users.", HttpStatus.OK, true);
    }

    @PostMapping("/config/role")
    public ResponseEntity<?> assignRole(@Valid @RequestBody UserWithRolesRequestDto rolesRequestDto,
            BindingResult result, HttpServletRequest request) {

        Long userId = getUserIdFromRequest(request);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        List<Long> roleIds = rolesRequestDto.getRoleIds();
        List<RoleEntity> roles = new ArrayList<>();
        for (Long id : roleIds) {
            RoleEntity role = roleRepository.findById(id);
            if (role != null) {
                roles.add(role);
            }
        }

        ResponseEntity<?> response = requestResponse(result, () -> this.userService.assignRolesToUser(rolesRequestDto),
                "Roles Updated.",
                HttpStatus.OK, true);

        String fullName = "";
        if (response.getBody() instanceof Map) {
            Map<?, ?> body = (Map<?, ?>) response.getBody();
            Object dataObj = body.get("data");
            if (dataObj instanceof UserWithRolesResponseDto) {
                fullName = ((UserWithRolesResponseDto) dataObj).getFullName();
            }
        }

        if (result == null || !result.hasErrors()) {
            List<Map<String, Object>> rolesDetail = roles.stream()
                    .map(role -> {
                        Map<String, Object> map = new LinkedHashMap<>();
                        map.put("ID", role.getId());
                        map.put("Rol", role.getName());
                        return map;
                    })
                    .collect(Collectors.toList());

            logGeneralService.createLog(userId, USUARIOS,
                    "Asignación de Roles a Usuario " + rolesRequestDto.getIdUser() + " : " + fullName,
                    rolesDetail);
        }

        return response;
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

    private String getToken(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        return header.split(" ")[1];
    }

    private Long getUserIdFromRequest(HttpServletRequest request) {
        String token = getToken(request);
        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            throw new SecurityException("User ID not Found.");
        }
        return userId;
    }

    @GetMapping("/register/{samAccountName}")
    public ResponseEntity<?> registerUserFromExternal(@PathVariable String samAccountName) {

        UserEntity user = userAddService.addUserIfNotExists(samAccountName);
        return requestResponse(user, "Usuario procesado correctamente.", HttpStatus.OK, true);

    }

}
