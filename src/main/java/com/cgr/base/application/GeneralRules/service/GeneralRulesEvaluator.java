package com.cgr.base.application.GeneralRules.service;

import java.math.BigDecimal;

import org.springframework.stereotype.Service;

// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.AmbitosCaptura;

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

    // Regla3: Presupuesto Inicial vs Definitivo.
    public String evaluateGeneralRule3(BigDecimal presupuestoDefinitivo, BigDecimal presupuestoInicial) {
        if (presupuestoDefinitivo == null || presupuestoInicial == null) {
            return "NO DATA";
        }
        return (presupuestoDefinitivo.compareTo(BigDecimal.ZERO) == 0
                && presupuestoInicial.compareTo(BigDecimal.ZERO) == 0) ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla4: Presupuesto Inicial por Periodos.
    public String evaluateGeneralRule4(BigDecimal period3Value, BigDecimal periodToCompare) {
        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }
        return periodToCompare.compareTo(period3Value) >= 0 ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla 5: Programación Gastos vs Ingresos.
    public String evaluateGeneralRule5(BigDecimal presupuestoInicial, BigDecimal apropiacionInicial) {

        if (presupuestoInicial == null || apropiacionInicial == null) {
            return "NO DATA";
        }

        return presupuestoInicial.compareTo(apropiacionInicial) == 0 ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla 5: Diferencia Apropiación vs Presupuesto.
    public BigDecimal calculateDifference(BigDecimal initialBudget, BigDecimal initialAllocation) {

        if (initialBudget == null || initialAllocation == null) {
            return null;
        }

        return initialBudget.subtract(initialAllocation);
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

    // // Regla 10: Validar Vigencia del Gasto en Ambitos Captura.
    // public Double getVigenciaFieldValue(String nombreVigenciaGasto, AmbitosCaptura matchedAmbito) {

    //     if (nombreVigenciaGasto == null || nombreVigenciaGasto.isEmpty()) {
    //         return null;
    //     }

    //     return switch (nombreVigenciaGasto) {
    //         case "VIGENCIA ACTUAL" -> {
    //             yield matchedAmbito.getVigenciaActual();
    //         }
    //         case "RESERVAS" -> {
    //             yield matchedAmbito.getReservas();
    //         }
    //         case "CUENTAS POR PAGAR" -> {
    //             yield matchedAmbito.getCxp();
    //         }
    //         case "VIGENCIAS FUTURAS - VIGENCIA ACTUAL" -> {
    //             yield matchedAmbito.getVfVa();
    //         }
    //         case "VIGENCIAS FUTURAS - RESERVAS" -> {
    //             yield matchedAmbito.getVfReserva();
    //         }
    //         case "VIGENCIAS FUTURAS - CUENTAS POR PAGAR" -> {
    //             yield matchedAmbito.getVfCxp();
    //         }
    //         default -> {
    //             yield null;
    //         }
    //     };
    // }

    // Regla10: Vigencia del Gasto - Programación Gastos.
    public String evaluateGeneralRule10(Double vigenciaValue) {
        if (vigenciaValue == null || vigenciaValue.isNaN()) {
            return "NO DATA";
        }
        return vigenciaValue == 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.0: Validacion Inexistencia Cuenta 2.3
    public String evaluateGeneralRule11_0(Boolean accountExists) {
        return accountExists ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla11.1: Validacion Existencia Cuenta 2.99
    public String evaluateGeneralRule11_1(Boolean accountExists) {
        return accountExists ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla12: Apropiacion Definitiva.
    public String evaluateGeneralRule12(BigDecimal appropriation) {
        if (appropriation == null) {
            return "NO DATA";
        }
        BigDecimal threshold = new BigDecimal("100000000");
        return appropriation.compareTo(threshold) < 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla13: Apropiacion Definitiva Cuentas Padre.
    public String evaluateGeneralRule13(String accountField, BigDecimal appropriation) {

        if (accountField != null && appropriation != null && !accountField.isEmpty()) {

            return switch (accountField) {
                case "2.1", "2.2", "2.4" -> {

                    if (!appropriation.equals(BigDecimal.ZERO)) {
                        yield "CUMPLE";
                    }
                    yield "NO CUMPLE";
                }
                default -> "NO DATA";
            };
        }
        return "NO DATA";
    }

    // Regla 14: Validación Apropiación Inicial por Periodos
    public String evaluateGeneralRule14(BigDecimal period3Value, BigDecimal periodToCompare, String codVig,
            String nameVig) {

        if (period3Value == null || periodToCompare == null) {
            return "NO DATA";
        }

        if (codVig.equals("1.0") && nameVig.equals("VIGENCIA ACTUAL")) {
            if (period3Value.compareTo(periodToCompare) == 0) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }
        if (codVig.equals("4.0") && nameVig.equals("VIGENCIA FUTURA")) {
            if (period3Value.compareTo(periodToCompare) == 0) {
                return "CUMPLE";
            }
            return "NO CUMPLE";
        }

        // Si no se cumple ninguna condición, devolver "NO DATA"
        return "NO DATA";
    }

    // Regla 15.0: Validación Compromisos VZ Obligaciones.
    public String evaluateGeneralRule15_0(BigDecimal compromisosValue, BigDecimal obligacionesValue) {
        if (compromisosValue == null || obligacionesValue == null) {
            return "NO DATA";
        }
        return compromisosValue.compareTo(obligacionesValue) < 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla 15.1: Validación Obligaciones VS Pagos.
    public String evaluateGeneralRule15_1(BigDecimal obligacionesValue, BigDecimal pagosValue) {
        if (obligacionesValue == null || pagosValue == null) {
            return "NO DATA";
        }
        return obligacionesValue.compareTo(pagosValue) < 0 ? "NO CUMPLE" : "CUMPLE";
    }

    // Regla 16: Validacion Cuenta Padre Gastos.
    public String evaluateGeneralRule16(Boolean existBudgetPlanning, Boolean existBudgetExecution) {
        if (existBudgetPlanning && existBudgetExecution) {
            return "CUMPLE";
        }
        return "NO CUMPLE";
    }

    // Regla17.0: Validacion Existencia Cuenta 2.3
    public String evaluateGeneralRule17_0(Boolean accountExists) {
        return accountExists ? "CUMPLE" : "NO CUMPLE";
    }

    // Regla17.1: Validacion Inexistencia Cuenta 2.99
    public String evaluateGeneralRule17_1(Boolean accountExists) {
        return accountExists ? "NO CUMPLE" : "CUMPLE";
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
