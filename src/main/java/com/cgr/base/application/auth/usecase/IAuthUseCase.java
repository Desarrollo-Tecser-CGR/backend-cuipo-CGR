package com.cgr.base.application.auth.usecase;

import java.util.Map;

import com.cgr.base.application.auth.dto.AuthRequestDto;
import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.application.auth.dto.UserDto;
import com.fasterxml.jackson.core.JsonProcessingException;

import jakarta.servlet.http.HttpServletRequest;

public interface IAuthUseCase {

    Map<String, Object> signIn(AuthRequestDto userRequest,  HttpServletRequest servletRequest ) throws JsonProcessingException;
     
    Map<String, Object> authWithLDAPActiveDirectory (AuthRequestDto userRequest, HttpServletRequest servletRequest) throws JsonProcessingException;
    // String signOut(String jwt);
    Map<String, Object> emailLogin (UserDto userRequest) throws JsonProcessingException;


    // Map<String, Object> checkStatusUser(String jwt) throws JsonProcessingException ;
    
}


