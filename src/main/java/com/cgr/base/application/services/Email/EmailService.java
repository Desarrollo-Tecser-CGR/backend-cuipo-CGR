package com.cgr.base.application.services.Email;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;
import org.thymeleaf.TemplateEngine;
import org.thymeleaf.context.Context;

import jakarta.mail.internet.MimeMessage;
import java.util.Map;

@Service
public class EmailService {

    @Autowired
    private JavaMailSender mailSender;

    @Autowired
    private TemplateEngine templateEngine;

    @Value("${email.from}")
    private String emailFrom;

    public void sendEmailFromTemplate(String to, String subject, Map<String, Object> model, boolean attachLogo) {
        try {
            MimeMessage mimeMessage = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "UTF-8");
            helper.setTo(to);
            helper.setSubject(subject);
            helper.setFrom(emailFrom);

            Context context = new Context();
            context.setVariables(model);

            String htmlContent = templateEngine.process("correo", context);
            helper.setText(htmlContent, true);

            if (attachLogo) {
                // Adjuntar el logo desde la carpeta 'static/images' (ajusta la ruta si es diferente)
                ClassPathResource logo = new ClassPathResource("static/imagenesMaps/logo.png");
                if (logo.exists()) {
                    helper.addInline("logo", logo);
                } else {
                    System.err.println("¡Advertencia! El logo no se encontró en la ruta especificada.");
                }
            }

            mailSender.send(mimeMessage);

        } catch (Exception e) {
            throw new RuntimeException("Error al enviar el correo desde la plantilla: " + e.getMessage());
        }
    }

    public void sendEmailFromTemplate(String to, String subject, Map<String, Object> model) {
        sendEmailFromTemplate(to, subject, model, false); // Llama a la versión sin adjuntar logo por defecto
    }
}