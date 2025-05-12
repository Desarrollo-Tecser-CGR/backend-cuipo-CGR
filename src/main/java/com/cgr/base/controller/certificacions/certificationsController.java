package com.cgr.base.controller.certificacions;

import static com.cgr.base.entity.logs.LogType.CERTIFICACIONES;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.service.certifications.CertificationsService;
import com.cgr.base.service.logs.LogGeneralService;

import jakarta.servlet.http.HttpServletRequest;

@PreAuthorize("hasAuthority('MENU_CERTIFY')")
@RestController
@RequestMapping("/api/v1/certifications")
public class certificationsController extends AbstractController {

    @Autowired
    private CertificationsService Certification;

    @Autowired
    private JwtService jwtService;
    @Autowired
    private LogGeneralService logGeneralService;

    @GetMapping("/options")
    public ResponseEntity<?> getUniqueEntities() {
        List<Map<String, String>> options = Certification.getUniqueEntities();
        return requestResponse(options, "List options successfully retrieved.", HttpStatus.OK, true);
    }

    @GetMapping("/info/{entidad}")
    public ResponseEntity<?> getRecordsByEntidad(@PathVariable("entidad") String entidad) {
        List<Map<String, Object>> records = Certification.getRecordsByCodigoEntidad(entidad);
        return requestResponse(records, "Records retrieved successfully.", HttpStatus.OK, true);
    }

    @PreAuthorize("hasAuthority('ROL_2')")
    @PutMapping("/update/calidad")
    public ResponseEntity<?> updateCalidad(@RequestBody Map<String, String> requestBody,
            HttpServletRequest httpRequest) {
        String header = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        String response = Certification.updateCertification(requestBody, userId, "calidad");
        logGeneralService.createLog(userId, CERTIFICACIONES, "Modificación de certificación de calidad " + requestBody);
        return requestResponse(response, "Update operation completed.", HttpStatus.OK, true);

    }

    @PreAuthorize("hasAuthority('ROL_2')")
    @PutMapping("/update/l617")
    public ResponseEntity<?> updateL617(@RequestBody Map<String, String> requestBody, HttpServletRequest httpRequest) {

        String header = httpRequest.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        String response = Certification.updateCertification(requestBody, userId, "L617");
        logGeneralService.createLog(userId, CERTIFICACIONES, "Modificación de certificación L617 " + requestBody);
        return requestResponse(response, "Update operation completed.", HttpStatus.OK, true);
    }

}
