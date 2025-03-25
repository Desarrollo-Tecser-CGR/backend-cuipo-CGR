package com.cgr.base.application.services.module.controllerModule;

import com.cgr.base.application.services.module.serviceModule.ServiceModule;
import com.cgr.base.application.services.module.serviceModule.dto.ContracViewDto;
import com.cgr.base.domain.dto.dtoEntityProvitionalPlan.EntityProvitionalPlanDto;
import com.cgr.base.infrastructure.utilities.DtoMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

@RequestMapping("api/v1/createModule")
@RestController
public class ControllerModule {

    private static final Logger logger = LoggerFactory.getLogger(ControllerModule.class);

    @Autowired
    private ServiceModule serviceModule;

    @Autowired
    private DtoMapper dtoMapper;

    @PostMapping(path = "create")
    public ResponseEntity<EntityProvitionalPlanDto> create(@RequestBody EntityProvitionalPlanDto provitionalPlanDto) {
        return serviceModule.create(provitionalPlanDto);
    }

    @GetMapping("/by-entity/{entityName}")
    public List<ContracViewDto> getContractsByEntity(@PathVariable String entityName) {
        try {
            // Registrar el valor recibido
            logger.info("🔹 EntityName recibido: {}", entityName);
            System.out.println("📌 EntityName recibido: " + entityName);

            // Decodificar en caso de que venga con caracteres codificados (ej: %20 en lugar de espacios)
            String decodedEntityName = URLDecoder.decode(entityName, StandardCharsets.UTF_8);
            logger.info("🔹 EntityName decodificado: {}", decodedEntityName);
            System.out.println("📌 EntityName decodificado: " + decodedEntityName);


            return serviceModule.getContractsByEntity(decodedEntityName);

        } catch (Exception e) {
            logger.error("❌ Error en getContractsByEntity: ", e);
            e.printStackTrace();
            return Collections.emptyList(); // Evita que la API devuelva un error 500
        }
    }

    @GetMapping("/all")
    public ResponseEntity<List<ContracViewDto>> getAllContractsTotal() {
        List<ContracViewDto> contracts = serviceModule.getContractsByEntityAll();

        if (contracts.isEmpty()) {
            return ResponseEntity.noContent().build(); // Retorna 204 si no hay datos
        }

        return ResponseEntity.ok(contracts); // Retorna 200 con la lista de contratos
    }




    @GetMapping("/by-total/{entityName}")
    public ResponseEntity<List<ContracViewDto>> getContractsByEntityTotals(@PathVariable String entityName) {
        try {
            if (entityName == null || entityName.trim().isEmpty()) {
                logger.warn("⚠️ EntityName es nulo o vacío");
                return ResponseEntity.badRequest().body(Collections.emptyList());
            }

            // Decodificar en caso de caracteres especiales (ej: %20 → espacio)
            String decodedEntityName = URLDecoder.decode(entityName, StandardCharsets.UTF_8);
            logger.info("🔹 Buscando contratos para la entidad: {}", decodedEntityName);

            List<ContracViewDto> result = serviceModule.getContractsByEntityTotals(decodedEntityName);

            if (result.isEmpty()) {
                logger.info("ℹ️ No se encontraron contratos para: {}", decodedEntityName);
                return ResponseEntity.status(HttpStatus.NO_CONTENT).body(result);
            }

            return ResponseEntity.ok(result);

        } catch (Exception e) {
            logger.error("❌ Error en getContractsByEntityTotals: {}", e.getMessage(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Collections.emptyList());
        }
    }




    @GetMapping("/allTotal")
    public ResponseEntity<List<ContracViewDto>> getAllContractsTotals() {
        List<ContracViewDto> contracts = serviceModule.getContractsByEntityAllTotals();

        if (contracts.isEmpty()) {
            return ResponseEntity.noContent().build();
        }

        return ResponseEntity.ok(contracts);
    }


}
