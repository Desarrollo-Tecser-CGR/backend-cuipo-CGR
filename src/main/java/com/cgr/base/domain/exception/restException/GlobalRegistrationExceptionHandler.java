package com.cgr.base.domain.exception.restException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.cgr.base.domain.exception.customException.MessageException;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import com.cgr.base.domain.exception.customException.InvalidVerificationTokenException;
import com.cgr.base.domain.exception.customException.ResourceNotFoundException;
import com.cgr.base.application.controller.AbstractController;

@RestControllerAdvice
public class GlobalRegistrationExceptionHandler extends AbstractController {

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(InvalidVerificationTokenException.class)
    public Map<String, String> tokenNotFound(InvalidVerificationTokenException ex) {
        Map<String, String> error = new HashMap<>();
        error.put("error", ex.getMessage());
        return error;
    }

    @ExceptionHandler({ ResourceNotFoundException.class, NoHandlerFoundException.class })
    public ResponseEntity<?> notFound(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({ HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class })
    public ResponseEntity<?> throwBadRequest(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler({ RuntimeException.class, Exception.class })
    public ResponseEntity<?> throwUnknownError(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<?> throwErrorMessage(Exception ex, HttpStatus httpStatus) {
        return requestResponse(null, ex.getMessage(), httpStatus, false);
    }

    @ExceptionHandler(MessageException.class)
    public ResponseEntity<?> handleBadRequestFromInterceptor(MessageException ex, HttpServletRequest request) {
        String acceptHeader = request.getHeader("Accept");
        if (acceptHeader != null && acceptHeader.contains(MediaType.TEXT_HTML_VALUE)) {
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .contentType(MediaType.TEXT_HTML)
                    .body(loadHtmlTemplate("error.html"));
        } else {
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("error", ex.getMessage()));
        }
    }

    // Manejamos los errores y devolvemos las vistas correspondientes en HTML
    @RequestMapping(value = "/error", produces = MediaType.TEXT_HTML_VALUE)
    @ResponseBody
    public ResponseEntity<String> handleErrorHtml(HttpServletRequest request) {
        Integer statusCode = (Integer) request.getAttribute("jakarta.servlet.error.status_code");
        if (statusCode != null && statusCode == HttpStatus.BAD_REQUEST.value()) {
            return new ResponseEntity<>(loadHtmlTemplate("error.html"), HttpStatus.BAD_REQUEST); // Carga el HTML correcto
        }
        return ResponseEntity.status(statusCode != null ? HttpStatus.valueOf(statusCode) : HttpStatus.INTERNAL_SERVER_ERROR)
                .contentType(MediaType.TEXT_HTML)
                .body("<h1>Error Inesperado</h1>");
    }

    private String loadHtmlTemplate(String templateName) {
        try {
            Path path = Paths.get("src/main/resources/templates/" + templateName);
            return Files.readString(path);
        } catch (IOException e) {
            e.printStackTrace();
            return "<h1>Error: No se pudo cargar la página de error</h1>";
        }
    }

}
