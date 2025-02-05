package com.cgr.base.application.GeneralRules.mapper;

import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataEjecGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;

@Service
public class mapperEntity {

    public GeneralRulesEntity mapToGeneralRulesEntity(Object data) {
        GeneralRulesEntity entity = new GeneralRulesEntity();

        if (data instanceof DataProgIngresos d) {
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        } else if (data instanceof DataProgGastos d) {
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        } else if (data instanceof DataEjecGastos d) {
            entity.setYear(extractYear(d.getPeriodo()));
            entity.setPeriod(extractPeriod(d.getPeriodo()));
            entity.setNameAmbit(d.getNombreAmbito());
            entity.setEntityName(d.getNombreEntidad());
            entity.setAccountName(d.getNombreCuenta());
        }

        return entity;
    }

    public String extractYear(String periodo) {
        return periodo != null && periodo.length() >= 4 ? periodo.substring(0, 4) : "";
    }

    public String extractPeriod(String periodo) {
        if (periodo != null && periodo.length() >= 6) {
            String month = periodo.substring(4, 6);
            return switch (month) {
                case "01", "02", "03" -> "3";
                case "04", "05", "06" -> "6";
                case "07", "08", "09" -> "9";
                case "10", "11", "12" -> "12";
                default -> "";
            };
        }
        return "";
    }

    public String generateKeyPeriod(GeneralRulesEntity entity) {
        return entity.getEntityName() + ":" + entity.getAccountName() + ":" + entity.getYear() + ":" + entity.getPeriod() + ":" + entity.getNameAmbit();
    }

    public String generateKeyYear(GeneralRulesEntity entity) {
        return entity.getEntityName() + ":" + entity.getAccountName() + ":" + entity.getYear() + ":" + entity.getNameAmbit();
     }

}
