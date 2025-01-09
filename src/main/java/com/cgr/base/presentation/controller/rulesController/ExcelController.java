package com.cgr.base.presentation.controller.rulesController;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.Rules.ExcelService;
import com.cgr.base.infrastructure.persistence.entity.Tables.InfGeneral;

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