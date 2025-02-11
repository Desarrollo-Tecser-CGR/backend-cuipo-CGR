package com.cgr.base.application.GeneralRules;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

// import com.cgr.base.infrastructure.persistence.entity.GeneralRules.GeneralRulesEntity;
// import com.cgr.base.infrastructure.persistence.repository.GeneralRules.GeneralRulesRepository;

// @Service
// public class GeneralRulesExportService {

//     @Autowired
//     private GeneralRulesRepository generalRulesRepository;

//     public ByteArrayOutputStream generateCsvStream() throws IOException {
//         List<GeneralRulesEntity> generalRulesData = generalRulesRepository.findAll();
//         String[] headers = getCsvHeader();

//         CSVFormat csvFormat = CSVFormat.DEFAULT
//                 .builder()
//                 .setQuoteMode(QuoteMode.ALL_NON_NULL)
//                 .setDelimiter(',')
//                 .setQuote('"')
//                 .setEscape('\\')
//                 .setNullString("")
//                 .setHeader(headers)
//                 .build();

//         try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
//              OutputStreamWriter writer = new OutputStreamWriter(byteArrayOutputStream);
//              CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat)) {

//             for (GeneralRulesEntity entidad : generalRulesData) {
//                 csvPrinter.printRecord(getCsvRecord(entidad));
//             }

//             writer.flush();
//             return byteArrayOutputStream;
//         }
//     }

//     private boolean shouldIncludeField(String fieldName) {
//         boolean startsWithGeneralRule = fieldName.startsWith("generalRule");
//         boolean endsWithPeriod = fieldName.matches(".*Period\\d+$");
        
//         return startsWithGeneralRule || !endsWithPeriod;
//     }

//     private String[] getCsvHeader() {
//         Field[] fields = GeneralRulesEntity.class.getDeclaredFields();
//         List<String> headersList = new ArrayList<>();
        
//         for (Field field : fields) {
//             String fieldName = field.getName();
//             if (shouldIncludeField(fieldName)) {
//                 headersList.add(fieldName);
//             }
//         }
        
//         return headersList.toArray(new String[0]);
//     }

//     private List<String> getCsvRecord(GeneralRulesEntity entidad) {
//         Field[] fields = GeneralRulesEntity.class.getDeclaredFields();
//         List<String> record = new ArrayList<>();

//         for (Field field : fields) {
//             String fieldName = field.getName();
//             if (shouldIncludeField(fieldName)) {
//                 try {
//                     field.setAccessible(true);
//                     Object value = field.get(entidad);
//                     record.add(value != null ? value.toString() : "");
//                 } catch (IllegalAccessException e) {
//                     record.add("ERROR");
//                 }
//             }
//         }

//         return record;
//     }
// }