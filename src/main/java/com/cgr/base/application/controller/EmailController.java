package com.cgr.base.application.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.cgr.base.application.services.Email.EmailService;


@RestController
public class EmailController {
    @Autowired
    private EmailService emailService;

    @Autowired
    @Qualifier("excel")


    @GetMapping("/EmailEnviar")
    public String sendEmail() {
        String to = "desarrollador5@tecsersas.com" ; 
        String subject = "Asunto predeterminado";
        String htmlContent = """
           <style>
    body {
        font-family: Arial, sans-serif;
        background-color: #f4f4f4;
        margin: 0;
        padding: 0;
    }
    .email-container {
        max-width: 600px;
        margin: 20px auto;
        background-color: #ffffff;
        border: 1px solid #dddddd;
        border-radius: 8px;
        padding: 20px;
    }
    .header {
        text-align: center;
        padding: 20px 0;
        border-bottom: 1px solid #dddddd;
    }
    .header h1 {
        color: #333333;
        margin: 0;
        font-size: 24px;
    }
    .content {
        padding: 20px;
        color: #555555;
        font-size: 16px;
        line-height: 1.6;
    }
    .content p {
        margin: 10px 0;
    }
    .button-container {
        text-align: center;
        margin: 20px 0;
    }
    .button {
        background-color: #007bff;
        color: #ffffff;
        padding: 10px 20px;
        text-decoration: none;
        border-radius: 5px;
        font-size: 16px;
    }
    .button:hover {
        background-color: #0056b3;
    }
    .footer {
        text-align: center;
        font-size: 12px;
        color: #999999;
        padding: 10px;
        border-top: 1px solid #dddddd;
    }
</style>
</head>
<body>
<div class="email-container">
    <div class="header">
        <h1>Confirmación de Correo Electrónico</h1>
    </div>

    <div>
        <img src="https://www.contraloria.gov.co/image/layout_set_logo?img_id=6251347&t=1731902840111">
    </div>

    <div class="content">
        <p>Estimado/a,</p>
        <p>Gracias por registrarte con nosotros. Por favor, confirma tu dirección de correo electrónico haciendo clic en el botón de abajo.</p>
        <div class="button-container">
            <a href="https://example.com/verificar?token=TOKEN_DE_VERIFICACION" class="button">Inrese Aqui</a>
        </div>
        <p>Si no solicitaste esta verificación, puedes ignorar este mensaje.</p>
        <p>Atentamente,<br>El equipo de [Tu Empresa]</p>
    </div>
    <div class="footer">
        <p>Este correo fue enviado automáticamente. Por favor, no respondas a este mensaje.</p>
        <p>&copy; 2024 [Tu Empresa]. Todos los derechos reservados.</p>
    </div>
</div>
</body>
            """;
        try {
          

            emailService.sendSimpleEmail(to, subject,htmlContent);
            return "Correo enviado con éxito a " + to;
        } catch (Exception e) {
            return "Error al enviar el correo: " + e.getMessage();
        }
    }

}