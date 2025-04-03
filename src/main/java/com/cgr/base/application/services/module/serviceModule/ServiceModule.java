package com.cgr.base.application.services.module.serviceModule;

import com.cgr.base.application.services.module.serviceModule.dto.ContracViewDto;
import com.cgr.base.domain.dto.dtoEntityProvitionalPlan.EntityProvitionalPlanDto;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import com.cgr.base.infrastructure.repositories.repositories.repositoryEntityProvitionalPlan.IEntityProvitionalPlanJpa;
import com.cgr.base.infrastructure.utilities.DtoMapper;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class ServiceModule {

        @PersistenceContext
        private EntityManager entityManager;

        @Autowired
        private DtoMapper dtoMapper;
        @Autowired
        private IEntityProvitionalPlanJpa entityProvitional;

        public ResponseEntity<EntityProvitionalPlanDto> create(EntityProvitionalPlanDto provitionalPlanDto) {

                EntityProvitionalPlan entity = dtoMapper.convertToDto(provitionalPlanDto, EntityProvitionalPlan.class);
                EntityProvitionalPlan savedEntity = entityProvitional.save(entity);

                EntityProvitionalPlanDto savedDto = new EntityProvitionalPlanDto();
                savedDto.setEntityNit(savedEntity.getEntityNit());
                savedDto.setEntityName(savedEntity.getEntityName());

                return ResponseEntity.status(HttpStatus.CREATED).body(savedDto);
        }

        // vista de consultas contratos con indicadores y nombre del indicador //
        // busqueda por nombre
        public List<ContracViewDto> getContractsByEntity(String entityName) {
                System.out.println("Buscando contratos para la entidad: " + entityName); // <-- Agrega este log

                String sql = "SELECT entity_id, entity_name, total_value_paid, indicator_name " +
                                "FROM vista_indicadores_contratos " +
                                "WHERE alias COLLATE Latin1_General_CI_AI = :alias";
                Query query = entityManager.createNativeQuery(sql);
                query.setParameter("alias", entityName);

                List<Object[]> results = query.getResultList();

                System.out.println("Resultados encontrados: " + results.size()); // <-- Otro log para ver si hay
                                                                                 // resultados

                return results.stream().map(row -> new ContracViewDto(
                                ((Integer) row[0]).longValue(), // entity_id
                                (String) row[1], // entity_name
                                ((Double) row[2]), // total_value_paid
                                (String) row[3]) // indicator_name
                ).collect(Collectors.toList());
        }

        // busqueda de todos con el nombre del indicador
        public List<ContracViewDto> getContractsByEntityAll() {
                String sql = "SELECT indicator_id, indicator_name, indicator_contract_id, entity_id, entity_name, alias, total_value_paid "
                                +
                                "FROM vista_indicadores_contratos";

                Query query = entityManager.createNativeQuery(sql);

                List<Object[]> results = query.getResultList();

                System.out.println("Resultados encontrados: " + results.size()); // Log para verificar la cantidad de
                                                                                 // resultados

                return results.stream().map(row -> new ContracViewDto(
                                ((Integer) row[3]).longValue(), // entity_id
                                (String) row[4], // entity_name
                                ((Double) row[6]), // total_value_paid
                                (String) row[1] // indicator_name
                )).collect(Collectors.toList());
        }

        // vista con la busqueda de la sumatoria de los contratos por el nombre
        public List<ContracViewDto> getContractsByEntityTotals(String entityName) {
                System.out.println("Buscando contratos para la entidad: " + entityName);

                String sql = "SELECT entity_id, entity_name, alias, total_value_paid " +
                                "FROM vista_indicadores_total_entidades " +
                                "WHERE alias COLLATE Latin1_General_CI_AI = :alias";

                Query query = entityManager.createNativeQuery(sql);
                query.setParameter("alias", entityName);

                List<Object[]> results = query.getResultList();

                System.out.println("Resultados encontrados: " + results.size());

                return results.stream().map(row -> new ContracViewDto(
                                ((Integer) row[0]).longValue(), // entity_id
                                (String) row[1],
                                ((Double) row[3]),
                                (String) row[2]

                )).collect(Collectors.toList());
        }

        // vista del total de los contratos
        public List<ContracViewDto> getContractsByEntityAllTotals() {
                String sql = "SELECT entity_id, entity_name, alias, total_value_paid " +
                                "FROM vista_indicadores_total_entidades ";

                Query query = entityManager.createNativeQuery(sql);

                List<Object[]> results = query.getResultList();

                System.out.println("Resultados encontrados: " + results.size()); // Log para verificar la cantidad de
                                                                                 // resultados

                return results.stream().map(row -> new ContracViewDto(
                                ((Integer) row[0]).longValue(), // entity_id
                                (String) row[1],
                                ((Double) row[3]),
                                (String) row[2]

                )).collect(Collectors.toList());
        }

}
