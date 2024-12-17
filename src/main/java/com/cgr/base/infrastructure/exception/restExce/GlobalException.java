package com.cgr.base.infrastructure.exception.restExce;

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

import com.cgr.base.infrastructure.exception.customExce.ResourceNotFoundException;
import com.cgr.base.infrastructure.exception.customExce.TokenException;
import com.cgr.base.presentation.controller.AbstractController;

@RestControllerAdvice
public class GlobalException extends AbstractController {

    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    @ExceptionHandler(TokenException.class)
    public Map<String, String> tokenNotFound(TokenException ex) {
        Map<String, String> error = new HashMap<>();
        error.put("error", ex.getMessage());
        return error;
    }

    @ExceptionHandler({ ResourceNotFoundException.class, NoHandlerFoundException.class })
    ResponseEntity<?> notFound(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler({ HttpMessageNotReadableException.class, MethodArgumentTypeMismatchException.class })
    ResponseEntity<?> throwBadRequest(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler({ RuntimeException.class, Exception.class })
    ResponseEntity<?> throwUnknownError(Exception ex) {
        return this.throwErrorMessage(ex, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    private ResponseEntity<?> throwErrorMessage(Exception ex, HttpStatus httpStatus) {
        return requestResponse(null, ex.getMessage(), httpStatus, false);
    }

}
