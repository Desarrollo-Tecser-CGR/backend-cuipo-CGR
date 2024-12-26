package com.test.testactivedirectory.application.auth.usecase;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.test.testactivedirectory.application.auth.dto.RegistrationRequestDto;
import com.test.testactivedirectory.application.auth.dto.AuthRequestDto;
import com.test.testactivedirectory.application.auth.dto.UserDto;

import jakarta.servlet.http.HttpServletRequest;

public interface IAuthUseCase {

    Map<String, Object> signIn(AuthRequestDto userRequest,  HttpServletRequest servletRequest ) throws JsonProcessingException;
     
    Map<String, Object> authWithLDAPActiveDirectory (AuthRequestDto userRequest, HttpServletRequest servletRequest) throws JsonProcessingException;
    // String signOut(String jwt);
    Map<String, Object> emailLogin (UserDto userRequest) throws JsonProcessingException;

    // Map<String, Object> checkStatusUser(String jwt) throws JsonProcessingException ;
    
    // String signOut(String jwt);
    
}



