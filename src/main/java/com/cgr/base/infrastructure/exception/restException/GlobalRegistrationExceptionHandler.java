package com.cgr.base.infrastructure.exception.restException;

import java.util.HashMap;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import com.cgr.base.infrastructure.exception.customException.InvalidVerificationTokenException;
import com.cgr.base.infrastructure.exception.customException.ResourceNotFoundException;
import com.cgr.base.presentation.controller.AbstractController;

@RestControllerAdvice
public class GlobalRegistrationExceptionHandler extends AbstractController {

    // Excepción para tokens de verificación inválidos.
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(InvalidVerificationTokenException.class)
    public Map<String, String> tokenNotFound(InvalidVerificationTokenException ex) {
        Map<String, String> error = new HashMap<>();
        error.put("error", ex.getMessage());
        return error;
    }

    // Excepción para recursos no encontrados.
    @ExceptionHandler({ ResourceNotFoundException.class, NoHandlerFoundException.class })
    ResponseEntity<?> notFound(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.NOT_FOUND);
    }

    // Excepciones para solicitudes incorrectas.
    @ExceptionHandler({ HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class })
    ResponseEntity<?> throwBadRequest(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.BAD_REQUEST);
    }

    // Excepciones generales y de tiempo de ejecución.
    @ExceptionHandler({ RuntimeException.class, Exception.class })
    ResponseEntity<?> throwUnknownError(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // Estandar para las respuestas de error.
    private ResponseEntity<?> throwErrorMessage(Exception ex, HttpStatus httpStatus) {
        return requestResponse(null, ex.getMessage(), httpStatus, false);
    }

}
