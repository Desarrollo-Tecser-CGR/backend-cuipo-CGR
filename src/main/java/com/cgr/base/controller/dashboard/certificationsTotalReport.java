package com.cgr.base.controller.dashboard;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.service.certifications.totalReport;

@RestController
@RequestMapping("/api/v1/dashboard")
public class certificationsTotalReport extends AbstractController {

    @Autowired
    totalReport Report;

    @GetMapping("/certifications")
    public ResponseEntity<?> getCertificationStats() {
        List<Map<String, Object>> stats = Report.getCertificationStats();
        return requestResponse(stats, "Certification statistics successfully retrieved.", HttpStatus.OK, true);
    }

}
