package com.test.testactivedirectory.application.Rules;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.test.testactivedirectory.application.Rules.dto.InfGeneralDto;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.DatosDept;
import com.test.testactivedirectory.infrastructure.persistence.entity.Tables.InfGeneral;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.InfoGeneralRepository;
import com.test.testactivedirectory.infrastructure.persistence.repository.tables.MiEntidadRepository;

@Service
public class Regla1Services {

    @Autowired
    private InfoGeneralRepository generalRepository;

    @Autowired
    private MiEntidadRepository departamentosRepository;

    @Transactional
    public Map<String, Object> rulesOne() {

        InfGeneralDto registroCumplenDto = new InfGeneralDto();
        Map<String, Object> json = new HashMap<>();
        Map<String, Object> regla1 = new HashMap<>();
        Map<String, Object> regla2 = new HashMap<>();
        int regla2Cumplen = 0;

        // Regla1
        List<InfGeneral> totalRegistros = this.generalRepository.findAll();
        List<DatosDept> datosDepts = this.departamentosRepository.findAll();

        List<InfGeneral> registrosCumplen = this.generalRepository.findInfGeneralCumpleRuleOne();
        for (InfGeneral registroCumplen : registrosCumplen) {
            // Regla 1
            registroCumplen.setRegla1("CUMPLE");
            registroCumplenDto.setIdCodigo(registroCumplen.getIdCodigo());
            registroCumplenDto.setCodigo(registroCumplen.getCodigo());

            // Regla 2
            if (this.departamentosRepository.existsByCodigoString(registroCumplen.getCodigo())) {
                registroCumplen.setRegla2("CUMPLE");
                regla2Cumplen++;
            } else {
                registroCumplen.setRegla2("NO CUMPLE");
            }

            generalRepository.save(registroCumplen);
        }

        List<InfGeneral> registrosNoCumplen = this.generalRepository.findInfGeneralNotCumpleRuleOne();
        for (InfGeneral registroNo : registrosNoCumplen) {
            // Regla 1
            registroNo.setRegla1("NO CUMPLE");
            generalRepository.save(registroNo);

            // Regla 2
            if (this.departamentosRepository.existsByCodigoString(registroNo.getCodigo())) {
                registroNo.setRegla2("CUMPLE");
                regla2Cumplen++;
            } else {
                registroNo.setRegla2("NO CUMPLE");
            }

            this.generalRepository.save(registroNo);
        }

        // Total
        regla1.put("cumplen", registrosCumplen.size());
        regla2.put("cumplen", regla2Cumplen);

        json.put("Total", totalRegistros.size());
        json.put("Validación código cuenta:", regla1);
        json.put("Validación código cuenta por ámbito:", regla2);
        return json;
    }

}
