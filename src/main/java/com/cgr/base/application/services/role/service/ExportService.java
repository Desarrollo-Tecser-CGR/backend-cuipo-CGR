package com.cgr.base.application.services.role.service;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.infrastructure.repositories.repositories.repositoryNotification.RepositoryNotification;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Cell;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.FileOutputStream;
import java.util.List;

@Service
public class ExportService {

    @Autowired
    private RepositoryNotification repositoryNotification;

    public String generatePdfWithDialog() {
        try {
            // 📌 Definir ruta fija sin JFileChooser
            String filePath = System.getProperty("user.home") + "/C:\\Users\\jhonn.baracaldo\\OneDrive - AIRES Y TECNOLOGIA SAS\\Documentos\\Prueba_Tec_Full_Stack\\Prueba";
            System.out.println("📂 Guardando PDF en: " + filePath);

            try (FileOutputStream fos = new FileOutputStream(filePath)) {
                PdfWriter writer = new PdfWriter(fos);
                PdfDocument pdf = new PdfDocument(writer);
                Document document = new Document(pdf);
                document.add(new Paragraph("Notificación"));

                List<EntityNotification> usuarios = repositoryNotification.findAll();
                Table table = new Table(5);
                table.addCell(new Cell().add(new Paragraph("ID")));
                table.addCell(new Cell().add(new Paragraph("Notificación")));
                table.addCell(new Cell().add(new Paragraph("Sujeto")));
                table.addCell(new Cell().add(new Paragraph("Fecha")));
                table.addCell(new Cell().add(new Paragraph("Número de contrato")));

                for (EntityNotification usuario : usuarios) {
                    table.addCell(new Cell().add(new Paragraph(String.valueOf(usuario.getId()))));
                    table.addCell(new Cell().add(new Paragraph(usuario.getNotification() != null ? usuario.getNotification() : "N/A")));
                    table.addCell(new Cell().add(new Paragraph(usuario.getSubject() != null ? usuario.getSubject() : "N/A")));
                    table.addCell(new Cell().add(new Paragraph(usuario.getDate() != null ? usuario.getDate().toString() : "N/A")));
                    table.addCell(new Cell().add(new Paragraph(usuario.getNumbercontract() != null ? usuario.getNumbercontract() : "N/A")));
                }

                document.add(table);
                document.close();

                System.out.println("✅ PDF guardado correctamente en: " + filePath);
                return filePath;

            }
        } catch (Exception e) {
            System.err.println("❌ Error al generar el PDF: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error al generar el PDF", e);
        }
    }
}
