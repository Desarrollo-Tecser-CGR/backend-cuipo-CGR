package com.cgr.base.infrastructure.persistence.entity.log;


public enum LogType {
    USUARIOS("usuarios"),
    CERTIFICACIONES("certificaciones"),
    PARAMETRIZACION("parametrización"),
    STRING;

    private final String value;

    LogType(String value) {
        this.value = value;
    }

    LogType() {
        this.value = null;
    }

    public String getValue() {
        return value;
    }
}
