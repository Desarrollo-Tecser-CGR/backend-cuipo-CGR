package com.cgr.base.application.rulesEngine;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.rulesEngine.GeneralRulesNames;
import com.cgr.base.infrastructure.persistence.repository.rulesEngine.generalRulesRepo;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityNotFoundException;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;

@Service
public class generalParameter {

    @PersistenceContext
    private EntityManager entityManager;

    @Autowired
    generalRulesRepo GeneralRepo;

    @Async
    @Transactional
    public void tableGeneralRulesName() {

        String checkTableQuery = """
                IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'GENERAL_RULES_NAMES')
                SELECT 1 ELSE SELECT 0;
                """;
        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                    CREATE TABLE GENERAL_RULES_NAMES (
                        CODIGO_REGLA VARCHAR(20) NOT NULL,
                        NOMBRE_REGLA VARCHAR(255) NOT NULL,
                        DESCRIPCION_REGLA VARCHAR(1000) NOT NULL
                    );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();

            String insertDataSQL = """
                    MERGE INTO GENERAL_RULES_NAMES AS target
                    USING (VALUES
                        ('REGLA_GENERAL_1', 'Presupuesto Definitivo para Entidades NO en Liquidación', 'Validar que el PRESUPUESTO_DEFINITIVO sea igual o mayor a $100.000.000 para la CUENTA 1, si el NOMBRE_CUENTA no termina en "en Liquidación".'),
                        ('REGLA_GENERAL_2', 'Presupuesto Inicial vs. Presupuesto Definitivo', 'Validar que el PRESUPUESTO_DEFINITIVO y el PRESUPUESTO_INICIAL no sean simultáneamente $0 en todas las cuentas.'),
                        ('REGLA_GENERAL_3', 'Presupuesto Inicial por Periodos', 'Validar que el PRESUPUESTO_INICIAL en los periodos 6, 9 y 12 sea igual al del periodo 3 en todas las cuentas.'),
                        ('REGLA_GENERAL_4A', 'Presupuesto Inicial vs. Apropiación Inicial', 'Validar que el PRESUPUESTO_INICIAL para CUENTA 1 sea igual a la suma de la APROPIACION_INICIAL en CUENTA 2 con COD_VIGENCIA_DEL_GASTO 1 o 4, solo para entidades con AMBITO_CODIGO entre 438 y 441.'),
                        ('REGLA_GENERAL_4B', 'Presupuesto Definitivo vs. Apropiación Definitiva', 'Validar que el PRESUPUESTO_DEFINITIVO para CUENTA 1 sea igual a la suma de la APROPIACION_DEFINITIVA en CUENTA 2 con COD_VIGENCIA_DEL_GASTO 1 o 4, solo para entidades con AMBITO_CODIGO entre 438 y 441.'),
                        ('REGLA_GENERAL_5', 'Recaudo Total por Periodos', 'Validar que el TOTAL_RECAUDO en los periodos 6, 9 y 12 sea superior al del periodo anterior hasta el tercer nivel de cuenta.'),
                        ('REGLA_GENERAL_6', 'Registro de Cuentas Padre para Ingresos', 'Validar que si existen registros de CUENTA "1.0", "1.1" y "1.2" en EJECUCION_INGRESOS, también existan en PROGRAMACION_INGRESOS.'),
                        ('REGLA_GENERAL_7', 'Sección Presupuestal', 'Validar que el COD_SECCION_PRESUPUESTAL corresponda a los definidos para AMBITO_CODIGO. Para Entidades Territoriales (438-441), debe estar entre 16 y 45; para Entidades Descentralizadas (438-441), entre 1 y 15.'),
                        ('REGLA_GENERAL_8', 'Vigencia del Gasto Programada', 'Validar que el NOM_VIGENCIA_DEL_GASTO o COD_VIGENCIA_DEL_GASTO correspondan, según AMBITO_CODIGO, con lo establecido en AMBITOS_CAPTURA.'),
                        ('REGLA_GENERAL_9A', 'Inexistencia de Cuenta "2.3" en Programación Gastos', 'Validar que ninguna cuenta inicie con "2.3". Si existen, notificar cuáles son.'),
                        ('REGLA_GENERAL_9B', 'Existencia de Cuenta "2.99" en Programación Gastos', 'Validar que exista al menos una cuenta "2.99". Si no, notificar cuáles faltan.'),
                        ('REGLA_GENERAL_10A', 'Vigencia de la Apropiación Definitiva', 'Validar que la APROPIACION_DEFINITIVA sea inferior a $100.000.000 para CUENTA 2, según AMBITO_CODIGO en AMBITOS_CAPTURA.'),
                        ('REGLA_GENERAL_10B', 'Apropiación Definitiva para Cuentas Padre', 'Validar que la APROPIACION_DEFINITIVA no sea $0 en CUENTA 2.1, 2.2 y 2.4.'),
                        ('REGLA_GENERAL_10C', 'Apropiación Definitiva para Cuenta "2.99"', 'Validar que la suma de APROPIACION_DEFINITIVA en CUENTA 2.99 sea superior a $100.000.000.'),
                        ('REGLA_GENERAL_11', 'Apropiación Inicial por Periodos', 'Validar que la suma de la APROPIACION_INICIAL en los periodos 6, 9 y 12 sea igual al del periodo 3 para COD_VIGENCIA_DEL_GASTO 1 y 4.'),
                        ('REGLA_GENERAL_12A', 'Compromisos vs. Obligaciones', 'Validar que en entidades que reportan compromisos, las OBLIGACIONES sean menores que los COMPROMISOS y calcular el porcentaje.'),
                        ('REGLA_GENERAL_12B', 'Obligaciones vs. Pago', 'Validar que los PAGOS sean menores que las OBLIGACIONES y calcular el porcentaje.'),
                        ('REGLA_GENERAL_13A', 'Registro de Cuentas Padre para Gastos', 'Validar que si existen cuentas "2.1", "2.2", "2.3" y "2.4" en EJECUCION_GASTOS, también existan en PROGRAMACION_GASTOS.'),
                        ('REGLA_GENERAL_13B', 'Inexistencia de Cuenta "2.99" en Ejecución', 'Validar que no existan registros de CUENTA con código "2.99".'),
                        ('REGLA_GENERAL_14A', 'Vigencia del Gasto Actual', 'Validar que existan registros con COD_VIGENCIA_DEL_GASTO 1 en los formularios de Programación y Ejecución de Gastos.'),
                        ('REGLA_GENERAL_14B', 'Consistencia de la Vigencia del Gasto', 'Validar que el campo COD_VIGENCIA_DEL_GASTO sea igual en Programación y Ejecución de Gastos.'),
                        ('REGLA_GENERAL_15', 'Código CPC', 'Validar que si la CUENTA tiene habilitada la variable CPC, el último dígito de la CUENTA coincida con el primer dígito de COD_CPC.'),
                        ('REGLA_GENERAL_16A', 'Variación de Gastos Trimestral', 'Validar que OBLIGACIONES o COMPROMISOS no tengan variaciones negativas mayores al 20% ni positivas mayores al 30% en los periodos 6, 9 y 12 respecto al anterior.'),
                        ('REGLA_GENERAL_16B', 'Variación de Gastos Anual', 'Validar variaciones en la ejecución de gastos respecto al año anterior.'),
                        ('REGLA_GENERAL_17A', 'Variación de Ingresos Trimestral', 'Validar que los ingresos no presenten variaciones anómalas en los periodos 6, 9 y 12 respecto al anterior.'),
                        ('REGLA_GENERAL_17B', 'Variación de Ingresos Anual', 'Validar variaciones en la ejecución de ingresos respecto al año anterior.')
                    ) AS source (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA)

                    ON target.CODIGO_REGLA = source.CODIGO_REGLA
                    WHEN NOT MATCHED THEN
                    INSERT (CODIGO_REGLA, NOMBRE_REGLA, DESCRIPCION_REGLA)
                    VALUES (source.CODIGO_REGLA, source.NOMBRE_REGLA, source.DESCRIPCION_REGLA);
                    """;
            entityManager.createNativeQuery(insertDataSQL).executeUpdate();
        }
    }

    public List<GeneralRulesNames> getAllRules() {
        return GeneralRepo.findAll();
    }

    public GeneralRulesNames updateRuleName(String codigoRegla, String nuevoNombre) {
        return GeneralRepo.findById(codigoRegla).map(regla -> {
            regla.setNombreRegla(nuevoNombre);
            return GeneralRepo.save(regla);
        }).orElseThrow(() -> new EntityNotFoundException("Regla no encontrada"));
    }

}