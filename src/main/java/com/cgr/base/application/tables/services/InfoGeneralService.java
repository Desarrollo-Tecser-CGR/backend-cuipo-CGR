package com.cgr.base.application.tables.services;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.infrastructure.persistence.entity.Tables.InfGeneral;
import com.cgr.base.infrastructure.persistence.repository.tables.InfoGeneralRepository;

@Service
public class InfoGeneralService {
    @Autowired
    private InfoGeneralRepository infoGeneralRepository;

    public void cargaGen(MultipartFile archivo) {
        try (BufferedReader lector = new BufferedReader(
                new InputStreamReader(archivo.getInputStream(), StandardCharsets.UTF_8));
                CSVParser parser = new CSVParser(lector, CSVFormat.DEFAULT
                        .withDelimiter(';') // Especificar el delimitador
                        .withHeader() // Usar la primera línea como encabezado
                        .withSkipHeaderRecord())) {

            for (CSVRecord record : parser) {
                InfGeneral entidad = new InfGeneral();
                entidad.setCodigo(record.get("CODIGO"));
                if (entidad.getCodigo().isEmpty()){
                    entidad.setCodigo(null); // Esto se insertará como NULL en la base de datos
                }
                    
                entidad.setNombre(record.get("NOMBRE"));
                if (entidad.getNombre().isEmpty()){
                    entidad.setNombre(null); // Esto se insertará como NULL en la base de datos
                }
                 
                entidad.setCPC(record.get("CPC"));
                if (entidad.getCPC().isEmpty()){
                    entidad.setCPC(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setDetalleSectorial(record.get("DETALLE_SECTORIAL"));
                if (entidad.getDetalleSectorial().isEmpty()){
                    entidad.setDetalleSectorial(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setFuentesFinanciacion(record.get("FUENTES_DE_FINANCIACION"));
                if (entidad.getFuentesFinanciacion().isEmpty()){
                    entidad.setFuentesFinanciacion(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setTerceros(record.get("TERCEROS"));
                if (entidad.getTerceros().isEmpty()){
                    entidad.setTerceros(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setPoliticaPublica(record.get("POLITICA_PUBLICA"));
                if (entidad.getPoliticaPublica().isEmpty()){
                    entidad.setPoliticaPublica(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setNumFechNorma(record.get("NUMERO_Y_FECHA_DE_LA_NORMA"));
                if (entidad.getNumFechNorma().isEmpty()){
                    entidad.setNumFechNorma(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setTipoNorma(record.get("TIPO_DE_NORMA"));
                if (entidad.getTipoNorma().isEmpty()){
                    entidad.setTipoNorma(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setReacau1(record.get("RECAUDO_VIGEN_ACTUAL_SIN_FONDOS"));
                if (entidad.getReacau1().isEmpty()){
                    entidad.setReacau1(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setReacau2(record.get("RECAUDO_VIGEN_ACTUAL_CON_FONDOS"));
                if (entidad.getReacau2().isEmpty()){
                    entidad.setReacau2(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setReacau3(record.get("RECAUDO_VIGEN_ANTERIOR_SIN_FONDO"));
                if (entidad.getReacau3().isEmpty()){
                    entidad.setReacau3(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setReacau4(record.get("RECAUDO_VIGEN_ANTERIOR_CON_FONDO"));
                if (entidad.getReacau4().isEmpty()){
                    entidad.setReacau4(null); // Esto se insertará como NULL en la base de datos
                }
                entidad.setTotalRecaudo(record.get("TOTAL_RECAUDO"));
                if (entidad.getTotalRecaudo() .isEmpty()){
                    entidad.setTotalRecaudo(null); // Esto se insertará como NULL en la base de datos
                }

                System.out.println(entidad);

                infoGeneralRepository.save(entidad);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error al procesar el archivo CSV: " + e.getMessage());
        }
    }

}