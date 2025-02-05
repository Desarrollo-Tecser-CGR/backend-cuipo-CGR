package com.cgr.base.application.GeneralRules.service;

import java.math.BigDecimal;

import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.GeneralRules.AmbitosCaptura;

@Service
public class GeneralRulesEvaluator {

    public String extractYearPeriod(String period) {
        return period.length() >= 4 ? period.substring(0, 4) : period;
    }

    public String extractPeriodByMonth(String dateString) {
        if (dateString == null || dateString.length() != 8) {
            return dateString;
        }
        String month = dateString.substring(4, 6);
        int monthNum;
        try {
            monthNum = Integer.parseInt(month);
        } catch (NumberFormatException e) {
            return dateString;
        }
        if (monthNum >= 1 && monthNum <= 3) {
            return "3";
        } else if (monthNum >= 4 && monthNum <= 6) {
            return "6";
        } else if (monthNum >= 7 && monthNum <= 9) {
            return "9";
        } else if (monthNum >= 10 && monthNum <= 12) {
            return "12";
        }
        return dateString;
    }

    // Regla1: Presupuesto Definitivo.
    public String evaluateGeneralRule1(BigDecimal budget) {
        if (budget == null) {
            return "NO DATA";
        }
        BigDecimal threshold = new BigDecimal("100000000");
        return budget.compareTo(threshold) < 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla2: Entidad en Liquidacion.
    public String evaluateGeneralRule2(String entity) {
        if (entity == null || entity.isEmpty()) {
            return "NO DATA";
        }
        return entity.toLowerCase().contains("liquidacion") ? "NO CUMPLE" : "CUMPLE";
     }

    // Regla3: Comparativo de Campos.
    public String evaluateGeneralRule3(BigDecimal presupuestoDefinitivo, BigDecimal presupuestoInicial) {
        if (presupuestoDefinitivo == null || presupuestoInicial == null) {
            return "NO DATA";
        }
        return (presupuestoDefinitivo.compareTo(BigDecimal.ZERO) == 0 
                && presupuestoInicial.compareTo(BigDecimal.ZERO) == 0) ? "NO CUMPLE" : "CUMPLE";
     }

    // Regla 4: Validación Presupuesto Inicial por Periodos
    public String evaluateGeneralRule4(String period3Value, String periodToCompare) {
        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }
        if (period3Value.isEmpty() || periodToCompare.isEmpty()) {
            return "NO DATA";
        }
        try {
            return Double.parseDouble(periodToCompare) >= Double.parseDouble(period3Value)
                    ? "CUMPLE"
                    : "NO CUMPLE";
        } catch (NumberFormatException e) {
            return "NO DATA";
        }
    }

    // Regla 5: Comparativo Ingresos.
    public String evaluateGeneralRule5(Double presupuestoInicialValue, Double apropiacionInicial) {
        if (presupuestoInicialValue == null || apropiacionInicial == null) {
            return "NO DATA";
        }
        if (presupuestoInicialValue.isNaN() || apropiacionInicial.isNaN()) {
            return "NO DATA";
        }

        BigDecimal presupuestoInicial = new BigDecimal(presupuestoInicialValue);
        BigDecimal apropiacion = new BigDecimal(apropiacionInicial);

        return presupuestoInicial.compareTo(apropiacion) == 0 ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla 5: Diferencia.
    public BigDecimal calculateDifference(Double initialBudgetValue, Double initialAllocation) {
        BigDecimal initialBudgetBigDecimal = new BigDecimal(initialBudgetValue);
        BigDecimal initialAllocationBigDecimal = new BigDecimal(initialAllocation);
        return initialBudgetBigDecimal.subtract(initialAllocationBigDecimal);
    }

    // Regla 8: Validación código sección presupuestal.
    public String evaluateGeneralRule8(String codigoAmbito, String codigoSeccionPresupuestal) {
        if (codigoAmbito != null && codigoSeccionPresupuestal != null) {

            if (!(codigoAmbito.isEmpty()) && !(codigoSeccionPresupuestal.isEmpty())) {

                String lastThreeDigits = codigoAmbito.length() >= 3
                        ? codigoAmbito.substring(codigoAmbito.length() - 3)
                        : null;

                int ambitoValue = Integer.parseInt(lastThreeDigits);
                double seccionValue = Double.parseDouble(codigoSeccionPresupuestal);

                if (ambitoValue >= 438 && ambitoValue <= 441) {
                    if (seccionValue >= 16.0 && seccionValue <= 45.0) {
                        return "CUMPLE";
                    } else {
                        return "NO CUMPLE";
                    }

                } else if (ambitoValue >= 442 && ambitoValue <= 454) {
                    if (seccionValue >= 1.0 && seccionValue <= 15.0) {
                        return "CUMPLE";
                    } else {
                        return "NO CUMPLE";
                    }
                } else {
                    return "NO DATA";
                }
            }
        }

        return "NO DATA";
    }

    // Regla 10: Validar Vigencia del Gasto en Ambitos Captura.
    public Double getVigenciaFieldValue(String nombreVigenciaGasto, AmbitosCaptura matchedAmbito) {

        if (nombreVigenciaGasto == null || nombreVigenciaGasto.isEmpty()) {
            return null;
        }

        return switch (nombreVigenciaGasto) {
            case "VIGENCIA ACTUAL" -> {
                yield matchedAmbito.getVigenciaActual();
            }
            case "RESERVAS" -> {
                yield matchedAmbito.getReservas();
            }
            case "CXP" -> {
                yield matchedAmbito.getCxp();
            }
            case "VF VA" -> {
                yield matchedAmbito.getVfVa();
            }
            case "VF RESERVA" -> {
                yield matchedAmbito.getVfReserva();
            }
            case "VF CXP" -> {
                yield matchedAmbito.getVfCxp();
            }
            default -> {
                yield null;
            }
        };
    }

    // Regla10: Validacion apropiacion definitiva definitivo.
    public String evaluateGeneralRule10(Double vigenciaValue) {
        if (vigenciaValue == null || vigenciaValue.isNaN()) {
            return "NO DATA";
        }
        return vigenciaValue == 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.0: Validacion Inexistencia Cuenta 2.3 .
    public String evaluateGeneralRule11_0(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.3") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.1: Validacion Existencia Cuenta 2.99 .
    public String evaluateGeneralRule11_1(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.99") ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla12: Validacion apropiacion definitiva definitivo.
    public String evaluateGeneralRule12(Double value) {
        if (value == null || value.isNaN()) {
            return "NO DATA";
        }
        return value < 100000000 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla13: Apropiacion definitiva diferente 0.
    public String evaluateGeneralRule13(String accountField, Double value) {
        if (accountField != null && value != null && !(accountField.isEmpty())) {

            return switch (accountField) {
                case "2.1", "2.2", "2.4" -> {
                    if (!value.equals(0.0)) {
                        yield "CUMPLE";
                    }
                    yield "NO CUMPLE";
                }
                default ->
                    "NO DATA";
            };
        }
        return "NO DATA";
    }

    // Regla 14: Validación Apropiación Inicial por Periodos
    public String evaluateGeneralRule14(String period3Value, String periodToCompare, String codVig, String nameVig) {
        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }
        if (period3Value.isEmpty() || periodToCompare.isEmpty()) {
            return "NO DATA";
        }
        if (codVig.equals("1.0") && nameVig.equals("VIGENCIA ACTUAL")) {
            if (period3Value.equals(periodToCompare)) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }
        if (codVig.equals("4.0") && nameVig.equals("VIGENCIA FUTURA")) {
            if (period3Value.equals(periodToCompare)) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }
        return "NO DATA";
    }

    // Regla 15.0: Validación Compromisos VZ Obligaciones.
    public String evaluateGeneralRule15_0(Double compromisosValue, Double obligacionesValue) {
        if (compromisosValue == null || obligacionesValue == null) {
            return "NO DATA";
        }
        if (compromisosValue.isNaN() || obligacionesValue.isNaN()) {
            return "NO DATA";
        }
        return compromisosValue < obligacionesValue ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla 15.1: Validación Obligaciones VS Pagos.
    public String evaluateGeneralRule15_1(Double obligacionesValue, Double pagosValue) {
        if (pagosValue == null || obligacionesValue == null) {
            return "NO DATA";
        }
        if (pagosValue.isNaN() || obligacionesValue.isNaN()) {
            return "NO DATA";
        }
        return obligacionesValue < pagosValue ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla 16: Validacion Cuenta Padre Gastos.
    public String evaluateGeneralRule16(String periodEjec, String periodProg, String cuentaProg, String cuentaEjec) {

        if (!isValidPeriod(periodEjec) || !isValidPeriod(periodProg)) {
            return "NO DATA";
        }
        if (periodEjec == null || periodProg == null) {
            return "NO DATA";
        }
        if (periodEjec.isEmpty() || periodProg.isEmpty()) {
            return "NO DATA";
        }
        if (cuentaProg == null || cuentaEjec == null) {
            return "NO DATA";
        }
        if (cuentaProg.isEmpty() || cuentaEjec.isEmpty()) {
            return "NO DATA";
        }
        if (periodEjec.equals(periodProg)) {
            if (cuentaProg.equals(cuentaEjec)) {
                return switch (cuentaProg) {
                    case "2.1", "2.2", "2.4" ->
                        "CUMPLE";
                    default ->
                        "NO CUMPLE";
                };
            }
        }
        return "NO CUMPLE";
    }

    public boolean isValidPeriod(String period) {
        return "3".equals(period) || "6".equals(period) || "9".equals(period) || "12".equals(period);
    }

    // Regla17.0: Validacion Inexistencia Cuenta 2.3 .
    public String evaluateGeneralRule17_0(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.3") ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla17.1: Validacion Existencia Cuenta 2.99 .
    public String evaluateGeneralRule17_1(String accountField) {
        if (accountField == null || accountField.isEmpty()) {
            return "NO DATA";
        }
        return accountField.equals("2.99") ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla 18: Identificación Vigencia Gasto.
    public String evaluateGeneralRule18(String codigoAmbito, String nombreVigencia) {
        if (codigoAmbito != null && nombreVigencia != null) {
            if (!(codigoAmbito.isEmpty()) || !(nombreVigencia.isEmpty())) {

                String lastThreeDigits = codigoAmbito.length() >= 3
                        ? codigoAmbito.substring(codigoAmbito.length() - 3)
                        : null;

                int ambitoValue = Integer.parseInt(lastThreeDigits);

                if (ambitoValue >= 442 && ambitoValue <= 454) {
                    if ("VIGENCIA ACTUAL".equals(nombreVigencia)) {
                        return "CUMPLE";
                    } else {
                        return "NO CUMPLE";
                    }
                } else {
                    return "NO DATA";
                }

            }
            return "NO DATA";

        }
        return "NO DATA";
    }

    // Regla19: Validacion Vigencia de Gastos.
    public String evaluateGeneralRule19(String vigenciaProg, String vigenciaEjec) {
        if (vigenciaProg == null || vigenciaEjec == null) {
            return "NO DATA";
        }
        if (vigenciaProg.isEmpty() || vigenciaEjec.isEmpty()) {
            return "NO DATA";
        }
        return vigenciaProg.equals(vigenciaEjec) ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla 20: Validación Variable CPC.
    public String evaluateGeneralRule20(String cuenta, String cpc) {
        if (cuenta == null || cpc == null) {
            return "NO DATA";
        }
        if (cuenta.isEmpty() || cpc.isEmpty()) {
            return "NO DATA";
        }
        char lastDigitCuenta = cuenta.charAt(cuenta.length() - 1);
        char firstDigitCpc = cpc.charAt(0);
        return lastDigitCuenta == firstDigitCpc ? "CUMPLE" : "NO CUMPLE";
    }

}
