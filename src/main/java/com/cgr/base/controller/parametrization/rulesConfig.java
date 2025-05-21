package com.cgr.base.controller.parametrization;

import static com.cgr.base.entity.logs.LogType.PARAMETRIZACION;

import java.util.LinkedHashMap;
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

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.entity.parametrization.GeneralRulesNames;
import com.cgr.base.entity.parametrization.SpecificRulesNames;
import com.cgr.base.entity.parametrization.SpecificRulesTables;
import com.cgr.base.service.logs.LogGeneralService;
import com.cgr.base.service.parametrization.generalParameter;
import com.cgr.base.service.parametrization.specificParameter;

import jakarta.servlet.http.HttpServletRequest;

@PreAuthorize("hasAuthority('MENU_PARAMETER')")
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
    public ResponseEntity<?> getAllRulesGeneral() {
        List<Map<String, Object>> result = serviceGR.getAllRules();
        return requestResponse(result, "Generals Rules successfully retrieved.", HttpStatus.OK, true);
    }

    @PostMapping("/general/rename/{codigoRegla}")
    public ResponseEntity<?> updateRuleNameGeneral(@PathVariable String codigoRegla,
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
            return ResponseEntity.badRequest().body("El codigo de reporte es obligatorio y no puede estar vacío.");
        }
        GeneralRulesNames updatedRule = serviceGR.updateRuleName(codigoRegla, nuevoNombre);

        Map<String, Object> detalle = new LinkedHashMap<>();
        detalle.put("Código", codigoRegla);
        detalle.put("NuevoNombre", request.get("nuevoNombre"));

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Modificación de Regla General: " + codigoRegla, detalle);

        return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);
    }

    @GetMapping("/specific/reports/details")
    public ResponseEntity<?> getAllSpecificRules() {
        List<SpecificRulesTables> result = serviceSR.getAllSpecificRules();
        return requestResponse(result, "Specific Rules successfully retrieved.", HttpStatus.OK, true);
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
        SpecificRulesTables updatedRule = serviceSR.updateReportName(codigoReporte, nuevoNombre);

        Map<String, Object> detalle = new LinkedHashMap<>();
        detalle.put("Código", codigoReporte);
        detalle.put("NuevoNombre", request.get("nuevoNombre"));

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Modificación de Reporte: " + codigoReporte, detalle);

        return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);

    }

    @GetMapping("/specific/details")
    public ResponseEntity<?> getAllRulesSpecific() {
        List<Map<String, Object>> result = serviceSR.getAllRules();
        return requestResponse(result, "Specific Rules successfully retrieved.", HttpStatus.OK, true);
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

        SpecificRulesNames updatedRule = serviceSR.updateRuleName(codigoRegla, nuevoNombre);

        Map<String, Object> detalle = new LinkedHashMap<>();
        detalle.put("Código", codigoRegla);
        detalle.put("NuevoNombre", request.get("nuevoNombre"));

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Modificación de Regla Específica: " + codigoRegla, detalle);

        return requestResponse(updatedRule, "Update operation completed.", HttpStatus.OK, true);

    }

}
