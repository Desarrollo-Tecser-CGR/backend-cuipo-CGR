package com.cgr.base.controller.parametrization;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.parameterization.generalParameter;
import com.cgr.base.application.parameterization.specificParameter;
import com.cgr.base.config.abstractResponse.AbstractController;
import static com.cgr.base.infrastructure.persistence.entity.log.LogType.PARAMETRIZACION;
import com.cgr.base.infrastructure.persistence.entity.parametrization.GeneralRulesNames;
import com.cgr.base.infrastructure.persistence.entity.parametrization.SpecificRulesNames;
import com.cgr.base.infrastructure.persistence.entity.parametrization.SpecificRulesTables;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;
import com.cgr.base.service.logs.LogGeneralService;

import jakarta.servlet.http.HttpServletRequest;

@PreAuthorize("hasAuthority('MENU_3')")
@RestController
@RequestMapping("/api/v1/parametrization/rules")
public class rulesConfig extends AbstractController {

    @Autowired
    private generalParameter serviceGR;

    @Autowired
    private specificParameter serviceSR;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private LogGeneralService logGeneralService;

    @GetMapping("/general/details")
    public List<GeneralRulesNames> getAllRulesGeneral() {
        return serviceGR.getAllRules();
    }

    @PostMapping("/general/rename/{codigoRegla}")
    public ResponseEntity<?> updateRuleNameGeneral(@PathVariable String codigoRegla, @RequestBody Map<String, String> request, HttpServletRequest request1) {
        String header = request1.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }
        String nuevoNombre = request.get("nuevoNombre");
        if (nuevoNombre == null || nuevoNombre.trim().isEmpty()) {
            return requestResponse(null, "El nuevo nombre es obligatorio.", HttpStatus.BAD_REQUEST, false);
        }
        if (codigoRegla == null || codigoRegla.trim().isEmpty()) {
            return ResponseEntity.badRequest().body("El codigo de reporte es obligatorio y no puede estar vacío.");
        }
        try {

            GeneralRulesNames updatedRule = serviceGR.updateRuleName(codigoRegla, nuevoNombre);

            logGeneralService.createLog(userId, PARAMETRIZACION,
                    "Modificación de regla general code " + codigoRegla + " to " + request.get("nuevoNombre"));

            return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }
    }

    @GetMapping("/specific/reports/details")
    public List<SpecificRulesTables> getAllSpecificRules() {
        return serviceSR.getAllSpecificRules();
    }

    @PostMapping("/specific/reports/rename/{codigoReporte}")
    public ResponseEntity<?> updateSpecificRuleName(@PathVariable String codigoReporte,
            @RequestBody Map<String, String> request, HttpServletRequest request1) {

        String header = request1.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }
        String nuevoNombre = request.get("nuevoNombre");
        if (nuevoNombre == null || nuevoNombre.trim().isEmpty()) {
            return requestResponse(null, "El nuevo nombre es obligatorio.", HttpStatus.BAD_REQUEST, false);
        }
        if (codigoReporte == null || codigoReporte.trim().isEmpty()) {
            return ResponseEntity.badRequest().body("El codigo de reporte es obligatorio y no puede estar vacío.");
        }
        try {

            SpecificRulesTables updatedRule = serviceSR.updateReportName(codigoReporte, nuevoNombre);

            logGeneralService.createLog(userId, PARAMETRIZACION,
                    "Modificación de reporte específico code " + codigoReporte + " to " + request.get("nuevoNombre"));

            return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }

    }

    @GetMapping("/specific/details")
    public List<SpecificRulesNames> getAllRulesSpecific() {
        return serviceSR.getAllRules();
    }

    @PostMapping("/specific/rename/{codigoRegla}")
    public ResponseEntity<?> updateRuleNameSpecific(@PathVariable String codigoRegla,
            @RequestBody Map<String, String> request, HttpServletRequest request1) {

        String header = request1.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }
        String nuevoNombre = request.get("nuevoNombre");
        if (nuevoNombre == null || nuevoNombre.trim().isEmpty()) {
            return requestResponse(null, "El nuevo nombre es obligatorio.", HttpStatus.BAD_REQUEST, false);
        }
        if (codigoRegla == null || codigoRegla.trim().isEmpty()) {
            return ResponseEntity.badRequest().body("El codigo de regla es obligatorio y no puede estar vacío.");
        }

        try {

            SpecificRulesNames updatedRule = serviceSR.updateRuleName(codigoRegla, nuevoNombre);

            logGeneralService.createLog(userId, PARAMETRIZACION,
                    "Modificación de regla específica code " + codigoRegla + " to " + request.get("nuevoNombre"));

            return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);

        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(null);
        }

    }

}
