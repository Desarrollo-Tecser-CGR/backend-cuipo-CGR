package com.cgr.base.entity.logs;

public enum LogType {
    USUARIOS("usuarios"),
    CERTIFICACIONES("certificaciones"),
    PARAMETRIZACION("parametrizacion"),
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
