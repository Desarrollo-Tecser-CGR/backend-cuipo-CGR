package com.cgr.base.application.GeneralRules.mapper;

import org.springframework.stereotype.Service;

// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataEjecGastos;
// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgGastos;
// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.DataProgIngresos;
// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;

@Service
public class mapperEntity {

    // public GeneralRulesEntity mapToGeneralRulesEntity(Object data) {
    //     GeneralRulesEntity entity = new GeneralRulesEntity();

    //     if (data instanceof DataProgIngresos d) {
    //         entity.setPeriodYear(extractYear(d.getPeriodo()));
    //         entity.setPeriodTrimester(extractPeriod(d.getPeriodo()));
    //         entity.setNameAmbit(d.getNombreAmbito());
    //         entity.setCodeAmbit(d.getCodigoAmbito());
    //         entity.setEntityName(d.getNombreEntidad());
    //         entity.setAccountName(d.getNombreCuenta());
    //         entity.setAccount(d.getCuenta());
    //     } else if (data instanceof DataProgGastos d) {
    //         entity.setPeriodYear(extractYear(d.getPeriodo()));
    //         entity.setPeriodTrimester(extractPeriod(d.getPeriodo()));
    //         entity.setNameAmbit(d.getNombreAmbito());
    //         entity.setCodeAmbit(d.getCodigoAmbito());
    //         entity.setEntityName(d.getNombreEntidad());
    //         entity.setAccountName(d.getNombreCuenta());
    //         entity.setAccount(d.getCuenta());
    //         entity.setCodeBudgetSection(d.getCodigoSeccionPresupuestal());
    //         entity.setValidProgName(d.getNombreVigenciaProg());
    //         entity.setValidProgCode(d.getCodVigenciaProg());
    //     } else if (data instanceof DataEjecGastos d) {
    //         entity.setPeriodYear(extractYear(d.getPeriodo()));
    //         entity.setPeriodTrimester(extractPeriod(d.getPeriodo()));
    //         entity.setNameAmbit(d.getNombreAmbito());
    //         entity.setCodeAmbit(d.getCodigoAmbito());
    //         entity.setEntityName(d.getNombreEntidad());
    //         entity.setAccountName(d.getNombreCuenta());
    //         entity.setAccount(d.getCuenta());
    //         entity.setCommitments(d.getCompromisos());
    //         entity.setObligations(d.getObligaciones());
    //         entity.setPayments(d.getPagos());
    //         entity.setValidExecName(d.getNombreVigenciaEjec());
    //         entity.setCodeCPC(d.getCodigoCPC());
    //     }

    //     return entity;
    // }

    // public String extractYear(String periodo) {
    //     return periodo != null && periodo.length() >= 4 ? periodo.substring(0, 4) : "";
    // }

    // public String extractPeriod(String periodo) {
    //     if (periodo != null && periodo.length() >= 6) {
    //         String month = periodo.substring(4, 6);
    //         return switch (month) {
    //             case "01", "02", "03" -> "3";
    //             case "04", "05", "06" -> "6";
    //             case "07", "08", "09" -> "9";
    //             case "10", "11", "12" -> "12";
    //             default -> "";
    //         };
    //     }
    //     return "";
    // }

    // public String generateKeyPeriod(GeneralRulesEntity entity) {
    //     return entity.getEntityName() + ":" + entity.getAccountName() + ":" + entity.getPeriodYear() + ":" + entity.getPeriodTrimester() + ":" + entity.getNameAmbit();
    // }

    // public String generateKeyYear(GeneralRulesEntity entity) {
    //     return entity.getEntityName() + ":" + entity.getAccountName() + ":" + entity.getPeriodYear() + ":" + entity.getNameAmbit();
    // }

    // public String generateKeyNoAccount(GeneralRulesEntity entity) {
    //     return entity.getEntityName() + ":" + entity.getPeriodYear() + ":" + entity.getPeriodTrimester() + ":" + entity.getNameAmbit();
    // }
    

}
