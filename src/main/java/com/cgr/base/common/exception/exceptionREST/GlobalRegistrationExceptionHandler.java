package com.cgr.base.common.exception.exceptionREST;

import org.apache.coyote.BadRequestException;
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

    // 401 - UNAUTHORIZED
    @ExceptionHandler({
            SecurityException.class,
            InvalidVerificationTokenException.class,
            IllegalArgumentException.class
    })
    public ResponseEntity<?> handleUnauthorized(Exception ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.UNAUTHORIZED, false);
    }

    // 403 - FORBIDDEN
    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<?> handleAccessDenied(AccessDeniedException ex) {
        return requestResponse(null, "Access denied: " + ex.getMessage(), HttpStatus.FORBIDDEN, false);
    }

    // 404 - NOT FOUND
    @ExceptionHandler({
            ResourceNotFoundException.class,
            EntityNotFoundException.class,
            NoHandlerFoundException.class
    })
    public ResponseEntity<?> handleNotFound(Exception ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.NOT_FOUND, false);
    }

    // 405 - METHOD NOT ALLOWED
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<?> handleMethodNotAllowed(HttpRequestMethodNotSupportedException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.METHOD_NOT_ALLOWED, false);
    }

    // 415 - UNSUPPORTED MEDIA TYPE
    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<?> handleUnsupportedMediaType(HttpMediaTypeNotSupportedException ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.UNSUPPORTED_MEDIA_TYPE, false);
    }

    // 500 - INTERNAL SERVER ERROR (Database)
    @ExceptionHandler(DataAccessException.class)
    public ResponseEntity<?> handleDatabaseError(DataAccessException ex) {
        return requestResponse(null, "Database error: " + ex.getMessage(), HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

    @ExceptionHandler({
            BadRequestException.class,
            HttpMessageNotReadableException.class,
            MethodArgumentTypeMismatchException.class,
            MissingServletRequestParameterException.class,
            BindException.class
    })
    public ResponseEntity<?> handleBadRequest(Exception ex) {
        return requestResponse(null, ex.getMessage(), HttpStatus.BAD_REQUEST, false);
    }

    // 500 - INTERNAL SERVER ERROR (Generic)
    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> handleUnexpected(Exception ex) {
        return requestResponse(null, "Unexpected error occurred.", HttpStatus.INTERNAL_SERVER_ERROR, false);
    }

}
