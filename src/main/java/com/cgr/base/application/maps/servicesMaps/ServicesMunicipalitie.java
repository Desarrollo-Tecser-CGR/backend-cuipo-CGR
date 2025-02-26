package com.cgr.base.application.maps.servicesMaps;

import com.cgr.base.application.maps.entity.dtoMaps.municipality.MunicipalityDto;
import com.cgr.base.application.maps.entity.dtoMaps.municipality.MunicipalityDtoDp;
import com.cgr.base.application.maps.entity.municipalities.EntityMunicipality;
import com.cgr.base.application.maps.repositoryMaps.RepositoryMunicipality;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
public class ServicesMunicipalitie {

    @Autowired
    private RepositoryMunicipality repositoryMunicipality;

    @Autowired
    private ServiceImage serviceImage;

    public ResponseEntity<Map<String, Object>> searchMunicipality(long id) {
        Map<String, Object> response = new HashMap<>();

        try {
            Optional<EntityMunicipality> searchMunicipality = repositoryMunicipality.findById(id);

            if (searchMunicipality.isPresent()) {
                MunicipalityDto dto = new MunicipalityDto();
                EntityMunicipality entity = searchMunicipality.get();

                dto.setMunicipality_id(entity.getMunicipality_id());
                dto.setMpio_cnmbr(entity.getMpio_cnmbr());
                dto.setMpio_ccnct(entity.getMpio_ccnct()!= null ? entity.getDpto_ccdgo() : "");

                String imageUrl = serviceImage.getImageUrlForDepartment(
                        entity.getMunicipality_id(),
                        entity.getMpio_cnmbr(),
                        dto.getMpio_ccnct() != null ? dto.getMpio_ccnct() : "default" // Usar ccdgo o valor por defecto
                );

                response.put("message", "Municipio encontrado.");
                response.put("data", dto);
                response.put("imageUrl", imageUrl);
                return new ResponseEntity<>(response, HttpStatus.OK);
            } else {
                response.put("message", "Municipio no encontrado para el ID: " + id);
                return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            response.put("message", "Error al buscar el municipio por ID: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    public ResponseEntity<Map<String, Object>> searchMunicipalityDp(String id) {
        Map<String, Object> response = new HashMap<>();

        try {
            Optional<EntityMunicipality> searchMunicipality = repositoryMunicipality.findByMpioCcdgo(id.trim());

            if (searchMunicipality.isPresent()) {
                MunicipalityDtoDp dto = new MunicipalityDtoDp();
                EntityMunicipality entity = searchMunicipality.get();

                dto.setMunicipality_id(entity.getMunicipality_id());
                dto.setMpio_cnmbr(entity.getMpio_cnmbr());
                dto.setMpio_ccnct(entity.getMpio_ccnct() != null ? entity.getMpio_ccnct() : "");

                String imageUrl = serviceImage.getImageUrlForDepartment(
                        entity.getMunicipality_id(),
                        entity.getMpio_cnmbr(),
                        id.trim() // Usar el ccdgo pasado como parámetro
                );

                response.put("message", "Municipio encontrado.");
                response.put("data", dto);
                response.put("imageUrl", imageUrl);
                return new ResponseEntity<>(response, HttpStatus.OK);
            } else {
                response.put("message", "Municipio no encontrado para el CCDGO: " + id);
                return new ResponseEntity<>(response, HttpStatus.NOT_FOUND);
            }
        } catch (Exception e) {
            response.put("message", "Error al buscar el municipio por CCDGO: " + e.getMessage());
            return new ResponseEntity<>(response, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}