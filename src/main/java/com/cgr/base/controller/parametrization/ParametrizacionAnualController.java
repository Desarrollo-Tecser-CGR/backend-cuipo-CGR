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
    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualService.getAll();
    }

    @GetMapping("/{fecha}")
    public ResponseEntity<ParametrizacionAnual> getByFecha(@PathVariable int fecha) {
        return parametrizacionAnualService.getByFecha(fecha)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
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
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de parametrización anual año " + parametrizacionAnual.getFecha() + " with values "
                        + parametrizacionAnual);
        return requestResponse(parametrizacionAnualService.save(parametrizacionAnual), "Create operation completed.",
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

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Modificación de parametrización anual año " + parametrizacionAnual.getFecha() + " to "
                        + parametrizacionAnual);

        return requestResponse(parametrizacionAnualService.update(parametrizacionAnual),
                "Update operation completed.", HttpStatus.OK, true);

    }

    @DeleteMapping("/{fecha}")
    public void deleteByFecha(@PathVariable int fecha) {
        parametrizacionAnualService.deleteByFecha(fecha);
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        String username = auth.getName();

    }

}
