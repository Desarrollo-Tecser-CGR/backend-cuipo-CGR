package com.test.testactivedirectory.application.Rules;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.InfGeneral;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.InfoGeneralRepository;

@Service
public class ExcelService {
    
   @Autowired
    private InfoGeneralRepository generalRepository;

    @Transactional
    public List <InfGeneral> ConsultExcel(){
        return(generalRepository.findAll());

    };
}
