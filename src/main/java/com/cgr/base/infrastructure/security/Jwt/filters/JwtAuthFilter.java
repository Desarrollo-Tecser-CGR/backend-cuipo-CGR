package com.cgr.base.infrastructure.security.Jwt.filters;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import com.cgr.base.application.auth.dto.AuthResponseDto;
import com.cgr.base.infrastructure.security.Jwt.providers.JwtAuthenticationProvider;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;
import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Component
public class JwtAuthFilter extends OncePerRequestFilter {

    private final JwtAuthenticationProvider jwtAuthenticationProvider;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private ObjectMapper getObjectMapper;

    @Autowired
    private IUserRepositoryJpa userRepositoryJpa;

    private List<String> urlsToSkip = List.of(
            "/api/v1/auth",
            "/api/v1/auth/**",
            "/auth",
            "/auth/",
            "/swagger-ui",
            "/v3/api-docs",
            "/api/v1/access/module/list",
            "/api/v1/access/module/roles",
            "/ws-endpoint");

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {

        String requestUri = request.getRequestURI();
        return urlsToSkip.stream().anyMatch(uri -> requestUri.startsWith(uri));
    }

    @SuppressWarnings("null")
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (header == null) {
            responseHandler(response, "Token is Required.", HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        if (header.isEmpty() || !header.startsWith("Bearer ") || header.split(" ").length != 2) {
            filterChain.doFilter(request, response);
            return;
        }

        String isTokenExpiredException = "";
        try {
            isTokenExpiredException = jwtService.isTokenExpired(header.split(" ")[1]);
        } catch (Exception e) {
            responseHandler(response, "Invalid Token.", HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        if (isTokenExpiredException != null) {

            responseHandler(response, isTokenExpiredException, HttpServletResponse.SC_FORBIDDEN);

            return;
        }

        String isTokenInvalidateFirma = jwtService.validateFirma(header.split(" ")[1]);

        if (isTokenInvalidateFirma != null) {
            responseHandler(response, isTokenInvalidateFirma, HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        boolean isEnableEmail = validateIsEnableEmail(header.split(" ")[1]);

        if (!isEnableEmail) {

            responseHandler(response, "User is not Enabled.", HttpServletResponse.SC_FORBIDDEN);

            return;
        }

        String validatetokeninlist = jwtAuthenticationProvider.validatetokenInlistToken(header.split(" ")[1]);
        if (validatetokeninlist != null) {

            responseHandler(response, validatetokeninlist, HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        String username = this.jwtService.getClaimUserName(header.split(" ")[1]);

        // Fetch roles from the database
        List<String> roles = userRepositoryJpa.findBySAMAccountNameWithRoles(username)
                .map(user -> user.getRoles().stream().map(role -> role.getName()).collect(Collectors.toList()))
                .orElseThrow(() -> new RuntimeException("User not found or roles not assigned."));

        // Obtener menús asociados a los roles
        List<Menu> menus = userRepositoryJpa.findMenusByRoleNames(roles);

        List<SimpleGrantedAuthority> authorities = roles.stream()
                .map(SimpleGrantedAuthority::new)
                .collect(Collectors.toList());

        // Agregar menús como authorities
        menus.forEach(menu -> authorities.add(new SimpleGrantedAuthority("MENU_" + menu.getId())));
        
        System.out.println("Authorities: " + authorities.toString());

        try {

            UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(
                    username, null, authorities);

            SecurityContextHolder.getContext().setAuthentication(auth);

        } catch (RuntimeException e) {
            SecurityContextHolder.clearContext();
            throw new RuntimeException(e);
        }

        filterChain.doFilter(request, response);
    }

    private void responseHandler(HttpServletResponse response, String exceptionHandler, int status) throws IOException {

        String message = getResponseJson(exceptionHandler);

        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(message);
        response.getWriter().flush();
    }

    private String getResponseJson(String isTokenExpired)
            throws JsonProcessingException {

        Map<String, Object> jsonresponse = new HashMap<>();

        jsonresponse.put("message", isTokenExpired);
        jsonresponse.put("statusCode", HttpServletResponse.SC_FORBIDDEN);
        jsonresponse.put("error", "Invalid Token.");

        String responseJson = getObjectMapper.writeValueAsString(jsonresponse);

        return responseJson;
    }

    private boolean validateIsEnableEmail(String token) throws JsonProcessingException {

        AuthResponseDto userDto = jwtService.getUserDto(token);

        if (userDto.getIsEnable()) {
            return true;
        }

        return false;
    }

}
