package com.cgr.base.controller.parametrization;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import static com.cgr.base.entity.logs.LogType.PARAMETRIZACION;
import com.cgr.base.entity.parametrization.ParametrizacionAnual;
import com.cgr.base.service.logs.LogGeneralService;
import com.cgr.base.service.parametrization.ParametrizacionAnualService;

import jakarta.servlet.http.HttpServletRequest;

@PreAuthorize("hasAuthority('MENU_PARAMETER')")
@RestController
@RequestMapping("/api/v1/parametrization/annual")
public class ParametrizacionAnualController extends AbstractController {

    @Autowired
    private ParametrizacionAnualService parametrizacionAnualService;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private LogGeneralService logGeneralService;

    @GetMapping
    public ResponseEntity<?> getAll() {
        return requestResponse(parametrizacionAnualService.getAll(),
                "Retornar Tabla Parametrizacion Anual", HttpStatus.OK, true);
    }

    @GetMapping("/{fecha}")
    public ResponseEntity<?> getByFecha(@PathVariable int fecha) {
        return requestResponse(parametrizacionAnualService.getByFecha(fecha),
                "Retornar Parametrizacion Anual por Fecha", HttpStatus.OK, true);
    }

    @PostMapping
    public ResponseEntity<?> create(@RequestBody ParametrizacionAnual parametrizacionAnual,
            HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        ParametrizacionAnual response = parametrizacionAnualService.save(parametrizacionAnual);
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de Parametrización Anual para el Año:" + parametrizacionAnual.getFecha(),
                parametrizacionAnual);
        return requestResponse(response, "Create operation completed.",
                HttpStatus.OK, true);
    }

    @PutMapping
    public ResponseEntity<?> update(@RequestBody ParametrizacionAnual parametrizacionAnual,
            HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        ParametrizacionAnual response = parametrizacionAnualService.update(parametrizacionAnual);

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Modificación de Parametrización Anual para el Año:" +
                        parametrizacionAnual.getFecha(),
                parametrizacionAnual);

        return requestResponse(response,
                "Update operation completed.", HttpStatus.OK, true);

    }

    @PreAuthorize("hasAuthority('ROL_1')")
    @DeleteMapping("/{fecha}")
    public ResponseEntity<?> deleteByFecha(@PathVariable int fecha, HttpServletRequest request) {
        parametrizacionAnualService.deleteByFecha(fecha);

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header != null && header.startsWith("Bearer ") ? header.split(" ")[1] : null;
        Long userId = token != null ? jwtService.extractUserIdFromToken(token) : null;

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Eliminación de Parametrización Anual para el Año:" + fecha, null);

        return requestResponse(null, "Delete operation completed.", HttpStatus.OK, true);
    }

}
