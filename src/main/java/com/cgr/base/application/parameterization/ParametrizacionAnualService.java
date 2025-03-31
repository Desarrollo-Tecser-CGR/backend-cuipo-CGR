package com.cgr.base.application.parameterization;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.parametrization.ParametrizacionAnual;
import com.cgr.base.infrastructure.persistence.repository.parametrization.ParametrizacionAnualRepository;

import jakarta.transaction.Transactional;

import java.math.BigDecimal;
import java.time.Year;

@Service
public class ParametrizacionAnualService {

    @Autowired
    private ParametrizacionAnualRepository parametrizacionAnualRepository;

    @Transactional
    public ParametrizacionAnual save(ParametrizacionAnual parametrizacionAnual) {
        if (!validarFecha(parametrizacionAnual.getFecha())) {
            throw new IllegalArgumentException(
                    "Solo se permite agregar o modificar el año actual o el inmediatamente anterior.");
        }

        calcularLimIcld(parametrizacionAnual);

        // Guardar el registro actual
        ParametrizacionAnual saved = parametrizacionAnualRepository.save(parametrizacionAnual);

        // Actualizar el año siguiente si corresponde
        actualizarAnioSiguiente(parametrizacionAnual);

        return saved;
    }

    @Transactional
    public ParametrizacionAnual update(ParametrizacionAnual parametrizacionAnual) {
        try {
            if (!validarFecha(parametrizacionAnual.getFecha())) {
                throw new IllegalArgumentException(
                        "Solo se permite agregar o modificar el año actual o el inmediatamente anterior.");
            }

            // Obtener el año anterior
            Optional<ParametrizacionAnual> anioAnteriorOpt = parametrizacionAnualRepository
                    .findByFecha(parametrizacionAnual.getFecha() - 1);

            if (anioAnteriorOpt.isPresent()) {
                ParametrizacionAnual anioAnterior = anioAnteriorOpt.get();
                BigDecimal ipcAnterior = anioAnterior.getIpc();

                if (ipcAnterior == null) {
                    throw new IllegalArgumentException("El campo IPC del año anterior no puede estar vacío.");
                }

                // Recalcular los valores del año actual basándose en el año anterior
                recalcularValoresAnioActual(parametrizacionAnual, anioAnterior, ipcAnterior);
            }

            // Guardar los cambios en el año actual
            ParametrizacionAnual updated = parametrizacionAnualRepository.update(parametrizacionAnual);

            // Actualizar el año siguiente si corresponde
            actualizarAnioSiguiente(parametrizacionAnual);

            return updated;
        } catch (Exception e) {
            // Log del error
            System.err.println("Error al actualizar ParametrizacionAnual: " + e.getMessage());
            e.printStackTrace();
            throw e; // Relanzar la excepción para que el cliente reciba el error
        }
    }

    public Optional<ParametrizacionAnual> getByFecha(int fecha) {
        return parametrizacionAnualRepository.findByFecha(fecha);
    }

    @Transactional
    public void deleteByFecha(int fecha) {
        parametrizacionAnualRepository.deleteByFecha(fecha);
    }

    public List<ParametrizacionAnual> getAll() {
        return parametrizacionAnualRepository.findAll();
    }

    private void calcularLimIcld(ParametrizacionAnual parametrizacionAnual) {
        int anioModificado = parametrizacionAnual.getFecha();
        Optional<ParametrizacionAnual> anioActual = parametrizacionAnualRepository.findByFecha(anioModificado);
        Optional<ParametrizacionAnual> anioSiguiente = parametrizacionAnualRepository.findByFecha(anioModificado + 1);

        if (anioSiguiente.isPresent() && anioActual.isPresent()) {
            ParametrizacionAnual actual = anioActual.get();

            // Asignar valor temporal si limIcld es null
            BigDecimal limIclDActual = actual.getLimIcld() != null ? actual.getLimIcld() : BigDecimal.ONE;
            BigDecimal ipcActual = parametrizacionAnual.getIpc();

            // Validar que ipc no sea null
            if (ipcActual == null) {
                throw new IllegalArgumentException("El campo IPC no puede estar vacío.");
            }

            BigDecimal nuevoLimIclD = limIclDActual
                    .add(limIclDActual.multiply(ipcActual.divide(BigDecimal.valueOf(100))));
            ParametrizacionAnual siguiente = anioSiguiente.get();
            siguiente.setLimIcld(nuevoLimIclD);

            parametrizacionAnualRepository.update(siguiente);
        }
    }

    private boolean validarFecha(int fecha) {
        int anioActual = Year.now().getValue();
        return fecha == anioActual || fecha == anioActual - 1;
    }

    private void actualizarAnioSiguiente(ParametrizacionAnual parametrizacionAnual) {
        int anioActual = parametrizacionAnual.getFecha();
        int anioSiguiente = anioActual + 1;

        // Calcular los valores del año siguiente basándose en el año actual
        Optional<ParametrizacionAnual> siguienteOpt = parametrizacionAnualRepository.findByFecha(anioSiguiente);

        if (siguienteOpt.isPresent()) {
            ParametrizacionAnual siguiente = siguienteOpt.get();

            BigDecimal limIcldActual = parametrizacionAnual.getLimIcld() != null ? parametrizacionAnual.getLimIcld()
                    : BigDecimal.ONE;
            BigDecimal ipcActual = parametrizacionAnual.getIpc(); // Usar el IPC del año actual

            if (ipcActual == null) {
                throw new IllegalArgumentException("El campo IPC no puede estar vacío.");
            }

            // Calcular el nuevo valor de limIcld para el año siguiente
            BigDecimal nuevoLimIcld = limIcldActual
                    .add(limIcldActual.multiply(ipcActual.divide(BigDecimal.valueOf(100))));
            siguiente.setLimIcld(nuevoLimIcld);

            // Actualizar las columnas adicionales
            actualizarValoresSesion(parametrizacionAnual, siguiente, ipcActual);

            // Actualizar el registro del año siguiente
            parametrizacionAnualRepository.update(siguiente);
        }
    }

    private void actualizarValoresSesion(ParametrizacionAnual actual, ParametrizacionAnual siguiente,
            BigDecimal ipcActual) {
        // Actualizar val_sesion_conc_e
        BigDecimal valSesionConcE = actual.getValSesionConcE() != null ? actual.getValSesionConcE() : BigDecimal.ONE;
        siguiente.setValSesionConcE(
                valSesionConcE.add(valSesionConcE.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_1
        BigDecimal valSesionConc1 = actual.getValSesionConc1() != null ? actual.getValSesionConc1() : BigDecimal.ONE;
        siguiente.setValSesionConc1(
                valSesionConc1.add(valSesionConc1.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_2
        BigDecimal valSesionConc2 = actual.getValSesionConc2() != null ? actual.getValSesionConc2() : BigDecimal.ONE;
        siguiente.setValSesionConc2(
                valSesionConc2.add(valSesionConc2.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_3
        BigDecimal valSesionConc3 = actual.getValSesionConc3() != null ? actual.getValSesionConc3() : BigDecimal.ONE;
        siguiente.setValSesionConc3(
                valSesionConc3.add(valSesionConc3.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_4
        BigDecimal valSesionConc4 = actual.getValSesionConc4() != null ? actual.getValSesionConc4() : BigDecimal.ONE;
        siguiente.setValSesionConc4(
                valSesionConc4.add(valSesionConc4.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_5
        BigDecimal valSesionConc5 = actual.getValSesionConc5() != null ? actual.getValSesionConc5() : BigDecimal.ONE;
        siguiente.setValSesionConc5(
                valSesionConc5.add(valSesionConc5.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));

        // Actualizar val_sesion_conc_6
        BigDecimal valSesionConc6 = actual.getValSesionConc6() != null ? actual.getValSesionConc6() : BigDecimal.ONE;
        siguiente.setValSesionConc6(
                valSesionConc6.add(valSesionConc6.multiply(ipcActual.divide(BigDecimal.valueOf(100)))));
    }

    private void recalcularValoresAnioActual(ParametrizacionAnual actual, ParametrizacionAnual anterior,
            BigDecimal ipcAnterior) {
        // Recalcular limIcld
        BigDecimal limIcldAnterior = anterior.getLimIcld() != null ? anterior.getLimIcld() : BigDecimal.ONE;
        BigDecimal nuevoLimIcld = limIcldAnterior
                .add(limIcldAnterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setLimIcld(nuevoLimIcld);

        // Recalcular valSesionConcE
        BigDecimal valSesionConcEAnterior = anterior.getValSesionConcE() != null ? anterior.getValSesionConcE()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConcE = valSesionConcEAnterior
                .add(valSesionConcEAnterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConcE(nuevoValSesionConcE);

        // Recalcular valSesionConc1
        BigDecimal valSesionConc1Anterior = anterior.getValSesionConc1() != null ? anterior.getValSesionConc1()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc1 = valSesionConc1Anterior
                .add(valSesionConc1Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc1(nuevoValSesionConc1);

        // Recalcular valSesionConc2
        BigDecimal valSesionConc2Anterior = anterior.getValSesionConc2() != null ? anterior.getValSesionConc2()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc2 = valSesionConc2Anterior
                .add(valSesionConc2Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc2(nuevoValSesionConc2);

        // Recalcular valSesionConc3
        BigDecimal valSesionConc3Anterior = anterior.getValSesionConc3() != null ? anterior.getValSesionConc3()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc3 = valSesionConc3Anterior
                .add(valSesionConc3Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc3(nuevoValSesionConc3);

        // Recalcular valSesionConc4
        BigDecimal valSesionConc4Anterior = anterior.getValSesionConc4() != null ? anterior.getValSesionConc4()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc4 = valSesionConc4Anterior
                .add(valSesionConc4Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc4(nuevoValSesionConc4);

        // Recalcular valSesionConc5
        BigDecimal valSesionConc5Anterior = anterior.getValSesionConc5() != null ? anterior.getValSesionConc5()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc5 = valSesionConc5Anterior
                .add(valSesionConc5Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc5(nuevoValSesionConc5);

        // Recalcular valSesionConc6
        BigDecimal valSesionConc6Anterior = anterior.getValSesionConc6() != null ? anterior.getValSesionConc6()
                : BigDecimal.ONE;
        BigDecimal nuevoValSesionConc6 = valSesionConc6Anterior
                .add(valSesionConc6Anterior.multiply(ipcAnterior.divide(BigDecimal.valueOf(100))));
        actual.setValSesionConc6(nuevoValSesionConc6);
    }
}