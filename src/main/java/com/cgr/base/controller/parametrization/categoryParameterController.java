package com.cgr.base.controller.parametrization;

import static com.cgr.base.entity.logs.LogType.PARAMETRIZACION;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.service.logs.LogGeneralService;
import com.cgr.base.service.parametrization.categoryParameter;

import jakarta.servlet.http.HttpServletRequest;

import java.util.*;

@PreAuthorize("hasAuthority('MENU_PARAMETER')")
@RestController
@RequestMapping("/api/v1/parametrization/category")
public class categoryParameterController extends AbstractController {

    @Autowired
    private categoryParameter categoryParameter;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private LogGeneralService logGeneralService;

    @PostMapping("/create/table/{year}")
    public ResponseEntity<?> createCategoryTableByYear(@PathVariable int year, HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        if (year < 2000 || year > 2500) {
            return requestResponse(null, "El año proporcionado no es válido.", HttpStatus.BAD_REQUEST, false);
        }

        categoryParameter.createYearlyTable(year);

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de tabla CATEGORIAS_ENTIDADES_" + year);

        return requestResponse(null, "Tabla creada exitosamente para el año " + year + ".", HttpStatus.CREATED, true);
    }

    @PostMapping("/create/record/{year}")
    public ResponseEntity<?> createRecord(
            @PathVariable int year,
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        categoryParameter.createRecordForYear(year, requestData);

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de registro en tabla CATEGORIAS_ENTIDADES_" + year + " con datos: " + requestData);

        return requestResponse(null, "Registro creado exitosamente.", HttpStatus.CREATED, true);
    }

    @PostMapping("/update/record/{year}")
    public ResponseEntity<?> updateRecord(
            @PathVariable int year,
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        categoryParameter.updateRecordForYear(year, requestData);
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Actualización de registro en tabla CATEGORIAS_ENTIDADES_" + year + " con datos: " + requestData);
        return requestResponse(null, "Registro actualizado exitosamente.", HttpStatus.OK, true);
    }

    @DeleteMapping("/delete/record/{year}")
    public ResponseEntity<?> deleteRecord(
            @PathVariable int year,
            @RequestParam String codigoEntidad,
            @RequestParam String ambitoCodigo,
            HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        categoryParameter.deleteRecordForYear(year, codigoEntidad, ambitoCodigo);
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Eliminación de registro en tabla CATEGORIAS_ENTIDADES_" + year + " con CODIGO_ENTIDAD: "
                        + codigoEntidad + " y AMBITO_CODIGO: " + ambitoCodigo);
        return requestResponse(null, "Registro eliminado exitosamente.", HttpStatus.OK, true);
    }

    @GetMapping("/list/{year}")
    public ResponseEntity<?> getRecordsByYear(@PathVariable int year) {
        List<Map<String, Object>> records = categoryParameter.getAllRecordsByYear(year);
        return requestResponse(records, null, HttpStatus.OK, true);
    }

    @GetMapping("/options")
    public ResponseEntity<?> getAvailableYears() {
        List<Integer> years = categoryParameter.getAvailableYears();
        return requestResponse(years, null, HttpStatus.OK, true);
    }

}
