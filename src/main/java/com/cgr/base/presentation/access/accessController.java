package com.cgr.base.presentation.access;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.access.service.accessManagement;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/access")
public class accessController extends AbstractController {

    @Autowired
    accessManagement Access;

    //Editar ejemplo de PreAuthorize
    //@PreAuthorize("hasAuthority('MENU_7') or hasAuthority('MENU_8')")
    @GetMapping("/module/list")
    public ResponseEntity<?> getAvailableMenus() {        
        List<Map<String, Object>> menus = Access.getAvailableMenus();
        return requestResponse(menus, "Available menus successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/module/roles")
    public ResponseEntity<?> getRolesWithMenus() {
        List<Map<String, Object>> rolesWithMenus = Access.getRolesWithMenus();
        return requestResponse(rolesWithMenus, "Roles and assigned modules successfully retrieved.", HttpStatus.OK,
                true);
    }

    @PutMapping("/config")
    public ResponseEntity<?> updateRoleModules(@RequestBody Map<String, Object> request) {

        if (!request.containsKey("roleId") || !request.containsKey("moduleIds")) {
            return requestResponse(null, "El roleId y la lista de módulos son obligatorios.", HttpStatus.BAD_REQUEST,
                    false);
        }

        try {
            Long roleId = ((Number) request.get("roleId")).longValue();
            List<Integer> moduleIds = ((List<?>) request.get("moduleIds")).stream()
                    .map(o -> ((Number) o).intValue())
                    .collect(Collectors.toList());

            boolean updated = Access.updateRoleModules(roleId, moduleIds);
            return updated ? requestResponse(null, "Módulos actualizados correctamente.", HttpStatus.OK, true)
                    : requestResponse(null, "Error al actualizar los módulos.", HttpStatus.INTERNAL_SERVER_ERROR,
                            false);
        } catch (Exception e) {
            e.printStackTrace();
            return requestResponse(null, "Error procesando la solicitud.", HttpStatus.INTERNAL_SERVER_ERROR, false);
        }
    }

}
