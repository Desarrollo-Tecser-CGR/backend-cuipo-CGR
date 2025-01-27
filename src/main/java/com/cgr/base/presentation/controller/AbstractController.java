package com.cgr.base.presentation.controller;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;

public abstract class AbstractController {

    protected ResponseEntity<?> requestResponse(BindingResult result, Supplier<?> action, String message,
            HttpStatus status, boolean successful) {
        if (result.hasFieldErrors()) {
            return buildValidationErrorResponse(result);
        }
        return buildResponse(action.get(), message, status, successful);
    }

    protected ResponseEntity<?> requestResponse(Object data, String message, HttpStatus status, boolean successful) {
        return buildResponse(data, message, status, successful);
    }

    private ResponseEntity<?> buildResponse(Object data, String message, HttpStatus status, boolean successful) {
        Map<String, Object> response = new HashMap<>();
        response.put("data", data);
        response.put("status", status.value());
        response.put("successful", successful);
        response.put(successful ? "message" : "error", message);
        return ResponseEntity.status(status).body(response);
    }

    private ResponseEntity<?> buildValidationErrorResponse(BindingResult result) {
        Map<String, String> errors = new HashMap<>();
        result.getFieldErrors().forEach(error -> errors.put(error.getField(),
                String.format("The field %s %s", error.getField(), error.getDefaultMessage())));
        return buildResponse(errors, "JSON Validation Error.", HttpStatus.BAD_REQUEST, false);
    }
}