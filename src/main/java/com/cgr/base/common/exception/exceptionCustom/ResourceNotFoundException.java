package com.cgr.base.common.exception.exceptionCustom;

public class ResourceNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ResourceNotFoundException(String message) {
        
        super(message);
        
    }

}
