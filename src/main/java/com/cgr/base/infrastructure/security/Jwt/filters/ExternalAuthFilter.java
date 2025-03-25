package com.cgr.base.infrastructure.security.Jwt.filters;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.cgr.base.application.auth.service.ExternalAuthService;
import com.cgr.base.application.auth.dto.AuthRequest;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
@Order(1)
public class ExternalAuthFilter extends OncePerRequestFilter {

    @Autowired
    private ExternalAuthService externalAuthService;

    @Autowired
    private ObjectMapper objectMapper;

    private List<String> urlsToNoSkip = List.of(
            "/api/v1/auth",
            "/api/v1/auth/**",
            "/auth",
            "/auth/",
            "/swagger-ui.html",
            "/swagger-ui");

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {
        String requestUri = request.getRequestURI();
        return urlsToNoSkip.stream().anyMatch(uri -> !requestUri.startsWith(uri));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        ContentCachingRequestWrapper wrappedRequest = new ContentCachingRequestWrapper(request);

        // Leer el cuerpo de la solicitud
        String requestBody = new BufferedReader(new InputStreamReader(request.getInputStream())).lines()
                .collect(Collectors.joining("\n"));

        // Deserializar el JSON a un objeto AuthRequest
        AuthRequest authRequest = objectMapper.readValue(requestBody, AuthRequest.class);

        String username = authRequest.getsAMAccountName();
        String password = authRequest.getPassword();

        System.out.println("Username: " + username);
        System.out.println("Password: " + password);

        if (username != null && password != null
                && externalAuthService.authenticateWithExternalService(username, password)) {
            // Si la autenticación es exitosa, se deja continuar con la cadena de filtros.

            System.out.println("Autenticación exitosa.");
            filterChain.doFilter(wrappedRequest, response);
        } else {
            response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            response.getWriter().write("{\"message\": \"Invalid credentials.\"}");
            response.getWriter().flush();
        }
    }
}
