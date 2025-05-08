package com.cgr.base.controller.access;

import java.util.List;
import java.util.Map;
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

        try {
            Long roleId = ((Number) request.get("roleId")).longValue();
            List<Integer> moduleIds = ((List<?>) request.get("moduleIds")).stream()
                    .map(o -> ((Number) o).intValue())
                    .collect(Collectors.toList());

            // Validate if the role exists
            if (!Access.roleExists(roleId)) {
                return requestResponse(null, "El rol especificado no existe.", HttpStatus.NOT_FOUND, false);
            }

            // Validate if all modules exist
            List<Integer> invalidModules = Access.getInvalidModules(moduleIds);
            if (!invalidModules.isEmpty()) {
                return requestResponse(null, "Los siguientes módulos no existen: " + invalidModules,
                        HttpStatus.BAD_REQUEST, false);
            }

            boolean updated = Access.updateRoleModules(roleId, moduleIds);

            logGeneralService.createLog(userId, USUARIOS,
                    "Modificación de rol id " + roleId + " con los módulos " + moduleIds + " asignados.");

            return updated ? requestResponse(null, "Módulos actualizados correctamente.", HttpStatus.OK, true)
                    : requestResponse(null, "Error al actualizar los módulos.", HttpStatus.INTERNAL_SERVER_ERROR,
                            false);
        } catch (Exception e) {
            return requestResponse(null, "Error procesando la solicitud.", HttpStatus.INTERNAL_SERVER_ERROR, false);
        }
    }

}
