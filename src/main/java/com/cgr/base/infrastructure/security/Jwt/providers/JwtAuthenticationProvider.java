package com.cgr.base.infrastructure.security.Jwt.providers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.stereotype.Component;

import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.common.exception.exceptionCustom.InvalidVerificationTokenException;
import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.fasterxml.jackson.core.JsonProcessingException;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@Component
public class JwtAuthenticationProvider {

    @Autowired
    private JwtService jwtService;

    @Autowired
    private IUserRepositoryJpa userRepositoryJpa;

    private HashMap<String, AuthResponseDto> listToken = new HashMap<>();


    public String createToken(AuthResponseDto customerJwt, List<RoleEntity> roles) throws JsonProcessingException {

        String tokenCreated = jwtService.createToken(customerJwt, roles);

        listToken.put(tokenCreated, customerJwt);

        return tokenCreated;
    }


    public Authentication createAuthentication(String token) throws AuthenticationException {

        try {

            AuthResponseDto exists = listToken.get(token);

            if (exists == null) {
                throw new BadCredentialsException("User is not Registered or session has not been Started.");

            }

            HashSet<SimpleGrantedAuthority> rolesAndAuthorities = new HashSet<>();

            // Obtener roles del usuario
            List<String> roleNames = jwtService.getRolesToken(token);

            // Obtener menús asociados a los roles
            List<Menu> menus = userRepositoryJpa.findMenusByRoleNames(roleNames);
            menus.forEach(menu -> rolesAndAuthorities.add(new SimpleGrantedAuthority("MENU_" + menu.getId())));

            return new UsernamePasswordAuthenticationToken(exists, token, rolesAndAuthorities);

        } catch (Exception e) {
            log.debug(token + " is invalid: " + e.getMessage());
            return null;
        }

    }

    public boolean validateToken(String token) throws JsonProcessingException {
        try {

            if (jwtService.validateFirma(token) != null)
                return false;
            if (validatetokenInlistToken(token) != null)
                return false;
            if (jwtService.validateToken(token))
                return false;

            return true;

        } catch (Exception e) {
            log.info("Error in Jwt Provider " + e.getMessage());
            throw e;
        }

    }

    public AuthResponseDto getUserDto(String token) throws JsonProcessingException {
        try {
            AuthResponseDto userDto = jwtService.getUserDto(token);
            return userDto;
        } catch (Exception e) {
            throw e;
        }

    }

    public String refresToken(String token) throws JsonProcessingException {
        try {
            AuthResponseDto userDto = jwtService.getUserDto(token);

            if (deleteToken(token).equals("session closed")) {
                List<RoleEntity> roles = new ArrayList<>();
                return createToken(userDto, roles);
            }
            return "";

        } catch (Exception e) {

            throw e;
        }

    }

    public String validatetokenInlistToken(String jwt) {

        if (listToken.containsKey(jwt)) {
            return null;
        }

        return "session closed";
    }

    public boolean validateIsEnableEmail(String token) {

        AuthResponseDto exists = listToken.get(token);

        if (exists == null) {
            throw new BadCredentialsException("User does not have an Active Account.");
        }

        return exists.getIsEnable();
    }

    public String deleteToken(String jwt) {

        if (!listToken.containsKey(jwt)) {

            throw new InvalidVerificationTokenException(
                    "Token does not exist, the user has Already Logged out.");
        }

        listToken.remove(jwt);

        return "session closed";
    }

}
