package com.cgr.base.presentation.specificRules;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.rules.general.specific.specificTablesInit;
import com.cgr.base.presentation.controller.AbstractController;

@RestController
@RequestMapping("/api/v1/rules/specific")
public class specificRules extends AbstractController {

    @Autowired
    private specificTablesInit specificInit;

    @PostMapping("/init-tables")
    public ResponseEntity<?> processTable() {
        specificInit.createTables();
        return requestResponse(
                null,
                "Tables Processing Completed.",
                HttpStatus.OK,
                true);
    }

}
