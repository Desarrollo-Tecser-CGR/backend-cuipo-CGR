package com.cgr.base.application.maps.controller;

import com.cgr.base.application.maps.entity.dtoMaps.deparment.DepartmentsDto;
import com.cgr.base.application.maps.servicesMaps.ServiceImage;
import com.cgr.base.application.maps.servicesMaps.ServicesDepartments;
import com.cgr.base.presentation.controller.AbstractController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RequestMapping(path = "/api/v1/departments")
@RestController
public class ControllerDepartment extends AbstractController {

    @Autowired
    private ServicesDepartments servicesDepartments;

    @Autowired
    private ServiceImage serviceImage;

    @GetMapping(path = "/{id}")
    public ResponseEntity<Map<String, Object>> departament(@PathVariable Long id) {
        return servicesDepartments.searchDepartment(id);
    }

    @GetMapping(path = "/dp/{id}")
    public ResponseEntity<Map<String, Object>> getDepartmentById(@PathVariable String id) {
        return servicesDepartments.searchDepartmenDp(id);
    }

    @GetMapping(path = "/all/dp/{id}")
    public ResponseEntity<Map<String, Object>> getDepartmentByAllId(@PathVariable Long id) {
        return servicesDepartments.searchDepartmenDpAll(id);
    }


}
