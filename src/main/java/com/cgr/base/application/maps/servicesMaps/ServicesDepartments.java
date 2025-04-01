package com.cgr.base.application.maps.servicesMaps;

import com.cgr.base.application.maps.entity.departments.EntityDepartments;
import com.cgr.base.domain.dto.dtoMaps.deparment.DepartmentsDto;
import com.cgr.base.domain.dto.dtoMaps.deparment.DepartmentsDtoDp;
import com.cgr.base.domain.dto.dtoMaps.municipality.MunicipalityDto;
import com.cgr.base.application.maps.repositoryMaps.RepositoryDepartment;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
/*
 * @Service
 * public class ServicesDepartments {
 * 
 * @Autowired
 * private RepositoryDepartment repositoryDepartment;
 * 
 * @Autowired
 * private ServiceImage serviceImage;
 * 
 * public ResponseEntity<Map<String, Object>> searchDepartment(Integer id) {
 * Map<String, Object> response = new HashMap<>();
 * 
 * try {
 * Optional<EntityDepartments> searchDepar = repositoryDepartment.findById(id);
 * 
 * if (searchDepar.isPresent()) {
 * DepartmentsDto dto = new DepartmentsDto();
 * EntityDepartments entity = searchDepar.get();
 * 
 * dto.setDepartment_id(entity.getDepartment_id());
 * dto.setDpto_cnmbr(entity.getDpto_cnmbr());
 * dto.setDpto_ccdgo(entity.getDpto_ccdgo() != null ? entity.getDpto_ccdgo() :
 * "");
 * 
 * String imageUrl = serviceImage.getImageUrlForDepartment(
 * entity.getDepartment_id(),
 * entity.getDpto_cnmbr(),
 * dto.getDpto_ccdgo() != null ? dto.getDpto_ccdgo() : "default"
 * );
 * 
 * response.put("message", "Departamento encontrado.");
 * response.put("data", dto);
 * response.put("imageUrl", imageUrl);
 * return new ResponseEntity<>(response, HttpStatus.OK);
 * } else {
 * response.put("message", "Departamento no encontrado para el ID: " + id);
 * return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
 * }
 * } catch (Exception e) {
 * response.put("message", "Error al buscar el departamento por ID: " +
 * e.getMessage());
 * return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
 * }
 * }
 * 
 * public ResponseEntity<Map<String, Object>> searchDepartmenDp(String id) {
 * try {
 * Optional<EntityDepartments> searchDepar =
 * repositoryDepartment.findByDpto_ccdgo(id.trim());
 * 
 * if (searchDepar.isPresent()) {
 * DepartmentsDtoDp dto = new DepartmentsDtoDp();
 * EntityDepartments entity = searchDepar.get();
 * 
 * dto.setDepartment_id(entity.getDepartment_id());
 * dto.setDpto_cnmbr(entity.getDpto_cnmbr());
 * dto.setDpto_ccdgo(entity.getDpto_ccdgo());
 * 
 * dto.setDpto_ccdgo(entity.getDpto_ccdgo() != null ? entity.getDpto_ccdgo() :
 * "");
 * 
 * String imageUrl = serviceImage.getImageUrlForDepartment(
 * entity.getDepartment_id(),
 * entity.getDpto_cnmbr(),
 * id.trim() // Usar el ccdgo pasado como parámetro
 * );
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Departamento encontrado.");
 * response.put("data", dto);
 * response.put("imageUrl", imageUrl);
 * return new ResponseEntity<>(response, HttpStatus.OK);
 * } else {
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Departamento no encontrado.");
 * return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
 * }
 * } catch (Exception e) {
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Error al buscar el departamento.");
 * return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
 * }
 * }
 * 
 * public ResponseEntity<Map<String, Object>> searchDepartmenDpAll(Long id) {
 * try {
 * Optional<EntityDepartments> searchDepar = repositoryDepartment.findById(id);
 * 
 * if (searchDepar.isPresent()) {
 * DepartmentsDto dto = new DepartmentsDto();
 * EntityDepartments entity = searchDepar.get();
 * 
 * dto.setDepartment_id(entity.getDepartment_id());
 * dto.setDpto_cnmbr(entity.getDpto_cnmbr());
 * dto.setDpto_ccdgo(entity.getDpto_ccdgo() != null ? entity.getDpto_ccdgo() :
 * "");
 * 
 * // Mapear los municipios relacionados
 * if (entity.getMunicipalitie() != null &&
 * !entity.getMunicipalitie().isEmpty()) {
 * dto.setMunicipalities(entity.getMunicipalitie().stream()
 * .map(m -> {
 * MunicipalityDto municipalityDto = new MunicipalityDto();
 * municipalityDto.setMunicipality_id(m.getMunicipality_id());
 * municipalityDto.setMpio_cnmbr(m.getMpio_cnmbr());
 * municipalityDto.setMpio_ccnct(m.getMpio_ccdgo() != null ? m.getMpio_ccdgo() :
 * "");
 * // Opcional: asignar image por municipio
 * // municipalityDto.setImage(serviceImage.getImageUrlForDepartment(
 * // m.getMunicipality_id(),
 * // m.getMpio_cnmbr(),
 * // m.getMpio_ccdgo() != null ? m.getMpio_ccdgo() : "default"
 * // ));
 * return municipalityDto;
 * })
 * .collect(Collectors.toList()));
 * }
 * 
 * String imageUrl = serviceImage.getImageUrlForDepartment(
 * entity.getDepartment_id(),
 * entity.getDpto_cnmbr(),
 * dto.getDpto_ccdgo() != null ? dto.getDpto_ccdgo() : "default"
 * );
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Departamento encontrado con sus municipios.");
 * response.put("data", dto);
 * response.put("imageUrl", imageUrl);
 * return new ResponseEntity<>(response, HttpStatus.OK);
 * } else {
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Departamento no encontrado para el ID: " + id);
 * return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
 * }
 * } catch (Exception e) {
 * Map<String, Object> response = new HashMap<>();
 * response.put("message", "Error al buscar el departamento por ID: " +
 * e.getMessage());
 * return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
 * }
 * }
 * 
 * }
 */
