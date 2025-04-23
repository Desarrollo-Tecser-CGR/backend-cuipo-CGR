package com.cgr.base.infrastructure.security.interceptor;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class HeadersInterceptor implements HandlerInterceptor {

    private String loadHtmlTemplate(String templateName) throws IOException {
        try {
            Path path = Paths.get("src/main/resources/templates/" + templateName);
            return Files.readString(path);
        } catch (IOException e) {
            e.printStackTrace();
            return "<h1>Error al cargar la página de error</h1>";
        }
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws IOException {
        String contentType = request.getHeader("Content-Type");
        String acceptHeader = request.getHeader("Accept");

        boolean errorOccurred = false;
        String errorMessage = null;
        int errorCode = HttpStatus.BAD_REQUEST.value();
        String responseContentType = MediaType.APPLICATION_JSON_VALUE;
        String responseBody = null;

        if (!request.getMethod().equalsIgnoreCase("GET") && (contentType == null || !contentType.equals("application/json"))) {
            errorMessage = "Error 400: Invalid Content-Type, expected application/json";
            errorOccurred = true;
        } else if (request.getHeader("Host") == null || request.getHeader("Host").isEmpty()) {
            errorMessage = "Error 400: Host header is required";
            errorOccurred = true;
        } else if (acceptHeader == null) {
            errorMessage = "Error 406: Invalid Accept header, expected application/json";
            errorCode = HttpStatus.NOT_ACCEPTABLE.value();
            errorOccurred = true;
        } else if (request.getHeader("User-Agent") == null || request.getHeader("User-Agent").isEmpty()) {
            errorMessage = "Error 400: User-Agent header is required";
            errorOccurred = true;
        }

        if (errorOccurred) {
            response.setStatus(errorCode);
            if (acceptHeader != null && acceptHeader.contains(MediaType.TEXT_HTML_VALUE)) {
                responseContentType = MediaType.TEXT_HTML_VALUE;
                responseBody = loadHtmlTemplate("error.html");
            } else {
                responseBody = String.format("{\"error\": \"%s\"}", errorMessage);
            }
            response.setContentType(responseContentType);
            response.getWriter().write(responseBody);
            response.getWriter().flush(); // Asegúrate de que se envíe
            return false; // Interrumpe la petición
        }

        return true;
    }
}