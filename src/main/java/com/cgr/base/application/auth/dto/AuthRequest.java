package com.cgr.base.application.auth.dto;

public class AuthRequest {

    private String sAMAccountName;
    private String password;

    public String getsAMAccountName() {
        return sAMAccountName;
    }

    public void setsAMAccountName(String sAMAccountName) {
        this.sAMAccountName = sAMAccountName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
