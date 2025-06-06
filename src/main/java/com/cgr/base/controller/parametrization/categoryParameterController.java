package com.cgr.base.controller.parametrization;

import static com.cgr.base.entity.logs.LogType.PARAMETRIZACION;

import org.apache.commons.lang3.ObjectUtils.Null;
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

    @PostMapping("/create/table")
    public ResponseEntity<?> createCategoryTableByYear(@RequestBody Map<String, Integer> body,
            HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        Integer year = body.get("year");
        if (year == null) {
            return requestResponse(null, "El campo 'year' es obligatorio.", HttpStatus.BAD_REQUEST, false);
        }

        categoryParameter.createYearlyTable(year);

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de Tabla CATEGORIAS ENTIDADES para el Año:" + year, null);

        return requestResponse(null, "Tabla creada exitosamente para el año " + year + ".", HttpStatus.CREATED, true);
    }

    @PostMapping("/create/record")
    public ResponseEntity<?> createRecord(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        Map<String, Object> newRecord = categoryParameter.createRecordForYear(requestData);

        int year = Integer.parseInt(requestData.get("year").toString());

        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Creación de Registro en Tabla CATEGORIAS ENTIDADES para el Año:" + year, newRecord);

        return requestResponse(newRecord, "Registro creado exitosamente.", HttpStatus.CREATED, true);
    }

    @PostMapping("/update/record")
    public ResponseEntity<?> updateRecord(
            @RequestBody Map<String, Object> requestData,
            HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return requestResponse(null, "User ID not found.", HttpStatus.FORBIDDEN, false);
        }

        int year = Integer.parseInt(requestData.get("year").toString());

        categoryParameter.updateRecordForYear(year, requestData);
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Actualización de Registro en Tabla CATEGORIAS ENTIDADES para el Año:" + year, requestData);

        return requestResponse(requestData, "Registro actualizado exitosamente.", HttpStatus.OK, true);
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

        Map<String, Object> detalle = new LinkedHashMap<>();
        detalle.put("Año", year);
        detalle.put("Código Entidad", codigoEntidad);
        detalle.put("Ámbito Código", ambitoCodigo);
        logGeneralService.createLog(userId, PARAMETRIZACION,
                "Eliminación de Registro en Tabla CATEGORIAS ENTIDADES para el Año:" + year,
                detalle);
        return requestResponse(detalle, "Registro eliminado exitosamente.", HttpStatus.OK, true);
    }

    @GetMapping("/list/{year}")
    public ResponseEntity<?> getRecordsByYear(@PathVariable int year) {
        List<Map<String, Object>> records = categoryParameter.getAllRecordsByYear(year);
        return requestResponse(records, "Registros retornados exitosamente", HttpStatus.OK, true);
    }

    @GetMapping("/options")
    public ResponseEntity<?> getAvailableYears() {
        List<Integer> years = categoryParameter.getAvailableYears();
        return requestResponse(years, "Años disponibles retornados exitosamente.", HttpStatus.OK, true);
    }

}
