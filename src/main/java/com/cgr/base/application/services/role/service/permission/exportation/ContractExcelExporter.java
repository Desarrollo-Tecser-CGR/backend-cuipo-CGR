package com.cgr.base.application.services.role.service.permission.exportation;



import com.cgr.base.application.notificaction.repository.RepositoryNotification;
import com.itextpdf.kernel.pdf.PdfDocument;
import com.itextpdf.kernel.pdf.PdfWriter;
import com.itextpdf.layout.Document;
import com.itextpdf.layout.element.Paragraph;
import com.itextpdf.layout.element.Table;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.ByteArrayOutputStream;
import java.util.List;
/*
@Service
public class ContractExcelExporter {

    @Autowired
    RepositoryNotification repositoryNotification;

    public byte[] generatePdf() {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            PdfWriter writer = new PdfWriter(out);
            PdfDocument pdf = new PdfDocument(writer);
            Document document = new Document(pdf);

            document.add(new Paragraph("Lista de Usuarios"));

            List<EntityNotification> usuarios = repositoryNotification.findAll();

            Table table = new Table(3); // 3 columnas
            table.addCell("ID");
            table.addCell("Nombre");
            table.addCell("Correo");

            for (Usuario usuario : usuarios) {
                table.addCell(usuario.getId().toString());
                table.addCell(usuario.getNombre());
                table.addCell(usuario.getCorreo());
            }

            document.add(table);
            document.close();

            return out.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("Error al generar el PDF", e);
        }
    }


}*/

