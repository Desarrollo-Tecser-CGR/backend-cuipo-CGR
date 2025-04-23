package com.cgr.base.common.exception.exceptionREST;

import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.validation.BindException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;
import org.springframework.web.servlet.NoHandlerFoundException;

import com.cgr.base.common.exception.exceptionCustom.InvalidVerificationTokenException;
import com.cgr.base.common.exception.exceptionCustom.ResourceNotFoundException;
import com.cgr.base.config.abstractResponse.AbstractController;

import jakarta.persistence.EntityNotFoundException;

@RestControllerAdvice
public class GlobalRegistrationExceptionHandler extends AbstractController {

    @ExceptionHandler(InvalidVerificationTokenException.class)
    public ResponseEntity<?> handleInvalidToken(InvalidVerificationTokenException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.UNAUTHORIZED, false);
    }

    @ExceptionHandler({
            ResourceNotFoundException.class,
            EntityNotFoundException.class,
            NoHandlerFoundException.class
    })
    public ResponseEntity<?> handleNotFound(Exception ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.NOT_FOUND, false);
    }

    @ExceptionHandler({
            IllegalArgumentException.class,
            IllegalStateException.class,
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class,
            BindException.class
    })
    public ResponseEntity<?> handleBadRequest(Exception ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.BAD_REQUEST, false);
    }

    @ExceptionHandler(SecurityException.class)
    public ResponseEntity<?> handleUnauthorized(SecurityException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.UNAUTHORIZED, false);
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<?> handleAccessDenied(AccessDeniedException ex) {
        return requestResponse(null, "Access denied: " + ex.getMessage(), HttpStatus.FORBIDDEN, false);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<?> handleMethodNotAllowed(HttpRequestMethodNotSupportedException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.METHOD_NOT_ALLOWED, false);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<?> handleUnsupportedMediaType(HttpMediaTypeNotSupportedException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.UNSUPPORTED_MEDIA_TYPE, false);
    }

    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<?> handleDatabaseError(DataAccessException ex) {
        return requestResponse(null, "Database error: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleUnexpected(Exception ex) {
        return requestResponse(null, "Unexpected error occurred.", HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

}
