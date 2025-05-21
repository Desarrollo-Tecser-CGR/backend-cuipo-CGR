package com.cgr.base.controller.access;

import java.util.LinkedHashMap;
import java.util.*;
import java.util.stream.Collectors;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.entity.menu.Menu;
import com.cgr.base.entity.role.RoleEntity;
import com.cgr.base.repository.menu.IMenuRepositoryJpa;
import com.cgr.base.repository.role.IRoleRepository;

import static com.cgr.base.entity.logs.LogType.USUARIOS;
import com.cgr.base.service.access.accessManagement;
import com.cgr.base.service.logs.LogGeneralService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/access")
public class accessController extends AbstractController {

    @Autowired
    accessManagement Access;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private LogGeneralService logGeneralService;

    @Autowired
    private IRoleRepository roleRepository;

    @Autowired
    private IMenuRepositoryJpa menuRepository;

    @PreAuthorize("hasAuthority('ROL_1')")
    @GetMapping("/module/list")
    public ResponseEntity<?> getAvailableMenus() {
        List<Map<String, Object>> menus = Access.getAvailableMenus();
        return requestResponse(menus, "Available menus successfully retrieved.", HttpStatus.OK, true);
    }

    @PreAuthorize("hasAuthority('ROL_1')")
    @GetMapping("/module/roles")
    public ResponseEntity<?> getRolesWithMenus() {
        List<Map<String, Object>> rolesWithMenus = Access.getRolesWithMenus();
        return requestResponse(rolesWithMenus, "Roles and assigned modules successfully retrieved.", HttpStatus.OK,
                true);
    }

    @PreAuthorize("hasAuthority('MENU_ACCESS')")
    @PutMapping("/config")
    public ResponseEntity<?> updateRoleModules(@RequestBody Map<String, Object> request,
            HttpServletRequest httpRequest) {

        String header = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (!request.containsKey("roleId") || !request.containsKey("moduleIds")) {
            return requestResponse(null, "El roleId y la lista de módulos son obligatorios.", HttpStatus.BAD_REQUEST,
                    false);
        }
        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        Long roleId = ((Number) request.get("roleId")).longValue();
        List<Integer> moduleIds = ((List<?>) request.get("moduleIds")).stream()
                .map(o -> ((Number) o).intValue())
                .collect(Collectors.toList());

        if (!Access.roleExists(roleId)) {
            return requestResponse(null, "El rol especificado no existe.", HttpStatus.NOT_FOUND, false);
        }

        List<Integer> invalidModules = Access.getInvalidModules(moduleIds);
        if (!invalidModules.isEmpty()) {
            return requestResponse(null, "Los siguientes módulos no existen: " + invalidModules,
                    HttpStatus.BAD_REQUEST, false);
        }

        boolean updated = Access.updateRoleModules(roleId, moduleIds);

        if (updated) {

            RoleEntity role = roleRepository.findById(roleId);
            String message = "Asignación de Módulos al Rol " + roleId + " : " + role.getName() + ".";

            List<Long> moduleIdsLong = moduleIds.stream()
                    .map(Integer::longValue)
                    .collect(Collectors.toList());

            List<Menu> menus = menuRepository.findAllById(moduleIdsLong);

            List<Map<String, Object>> modulesDetail = menus.stream()
                    .map(menu -> {
                        Map<String, Object> map = new LinkedHashMap<>();
                        map.put("ID", menu.getId());
                        map.put("Modulo", menu.getTitle());
                        return map;
                    })
                    .collect(Collectors.toList());

            logGeneralService.createLog(userId, USUARIOS,
                    message, modulesDetail);

        }

        return requestResponse(null, "Módulos actualizados correctamente.", HttpStatus.OK, true);
    }

}
