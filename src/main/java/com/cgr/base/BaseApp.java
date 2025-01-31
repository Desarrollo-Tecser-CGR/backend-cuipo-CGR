package com.cgr.base;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import io.github.cdimascio.dotenv.Dotenv;

@SpringBootApplication
public class BaseApp {

    public static void main(String[] args) {

        // Cargar variables de entorno desde el archivo .env
        Dotenv dotenv = Dotenv.configure()
                .directory("./")
                .ignoreIfMissing()
                .load();

        // Establecer las variables de entorno como propiedades del sistema
        dotenv.entries().forEach(entry -> {
            System.setProperty(entry.getKey(), entry.getValue());
        });

        // Iniciar la aplicación Spring Boot
        SpringApplication.run(BaseApp.class, args);
    }
}