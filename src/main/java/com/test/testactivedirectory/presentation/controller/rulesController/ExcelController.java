package com.test.testactivedirectory.presentation.controller.rulesController;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.test.testactivedirectory.application.Rules.ExcelService;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.InfGeneral;

@RestController
@RequestMapping("/api/excel")
public class ExcelController {

    @Autowired
    private ExcelService excelService;

    @GetMapping()
    public List<InfGeneral> getAll() {
        return this.excelService.ConsultExcel();
    }

}
