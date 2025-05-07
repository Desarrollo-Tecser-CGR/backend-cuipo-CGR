package com.cgr.base;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;

import java.util.Map;

@SpringBootApplication
@EnableScheduling
@EnableAsync
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

    @ControllerAdvice
    public static class GlobalExceptionHandler {

        @ExceptionHandler(AccessDeniedException.class)
        public ResponseEntity<?> handleAccessDeniedException(AccessDeniedException ex) {
            String message = ex.getMessage();

            if (message.contains("not found")) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
                        "data", null,
                        "error", "Resource not found: " + message,
                        "status", HttpStatus.NOT_FOUND.value(),
                        "successful", false));
            }

            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of(
                    "data", null,
                    "error", "Bad Request: " + message,
                    "status", HttpStatus.BAD_REQUEST.value(),
                    "successful", false));
        }
    }
}