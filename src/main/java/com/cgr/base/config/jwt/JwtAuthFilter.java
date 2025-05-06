package com.cgr.base.config.jwt;

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

import com.cgr.base.dto.auth.AuthResponseDto;
import com.cgr.base.entity.menu.Menu;
import com.cgr.base.repository.user.IUserRepositoryJpa;
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
            "/ws-endpoint");

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) throws ServletException {

        String requestUri = request.getRequestURI();
        return urlsToSkip.stream().anyMatch(uri -> requestUri.startsWith(uri));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);

        if (header == null) {
            responseHandler(response, "Token is Required.", HttpServletResponse.SC_UNAUTHORIZED); // 401 for missing
                                                                                                  // token
            return;
        }

        if (header.isEmpty() || !header.startsWith("Bearer ") || header.split(" ").length != 2) {
            filterChain.doFilter(request, response);
            return;
        }

        String token = header.split(" ")[1];

        try {
            if (jwtService.isTokenExpired(token) != null) {
                responseHandler(response, "Token has expired.", HttpServletResponse.SC_UNAUTHORIZED); // 401 for expired
                                                                                                      // token
                return;
            }

            if (jwtService.validateFirma(token) != null) {
                responseHandler(response, "Invalid Token Signature.", HttpServletResponse.SC_UNAUTHORIZED); // 401 for
                                                                                                            // invalid
                                                                                                            // signature
                return;
            }

            if (!validateIsEnableEmail(token)) {
                responseHandler(response, "User is not Enabled.", HttpServletResponse.SC_FORBIDDEN); // 403 for disabled
                                                                                                     // user
                return;
            }

            String username = jwtService.getClaimUserName(token);

            // Fetch roles and permissions
            List<String> roles = userRepositoryJpa.findBySAMAccountNameWithRoles(username)
                    .map(user -> user.getRoles().stream().map(role -> role.getName()).collect(Collectors.toList()))
                    .orElseThrow(() -> new RuntimeException("User not found or roles not assigned."));

            List<Menu> menus = userRepositoryJpa.findMenusByRoleNames(roles);

            List<SimpleGrantedAuthority> authorities = roles.stream()
                    .map(SimpleGrantedAuthority::new)
                    .collect(Collectors.toList());

            menus.forEach(menu -> authorities.add(new SimpleGrantedAuthority("MENU_" + menu.getCode())));

            UsernamePasswordAuthenticationToken auth = new UsernamePasswordAuthenticationToken(
                    username, null, authorities);

            SecurityContextHolder.getContext().setAuthentication(auth);

        } catch (RuntimeException e) {
            SecurityContextHolder.clearContext();
            responseHandler(response, "Access Denied.", HttpServletResponse.SC_FORBIDDEN); // 403 for access denied
            return;
        }

        filterChain.doFilter(request, response);
    }

    private void responseHandler(HttpServletResponse response, String exceptionHandler, int status) throws IOException {

        String message = getResponseJson(exceptionHandler, status);

        response.setContentType("application/json");
        response.setStatus(status);
        response.getWriter().write(message);
        response.getWriter().flush();
    }

    private String getResponseJson(String message, int status)
            throws JsonProcessingException {

        Map<String, Object> jsonresponse = new HashMap<>();

        jsonresponse.put("message", message);
        jsonresponse.put("statusCode", status); // Ensure correct status code is set
        jsonresponse.put("error", status == HttpServletResponse.SC_UNAUTHORIZED ? "Unauthorized" : "Forbidden");

        return getObjectMapper.writeValueAsString(jsonresponse);
    }

    private boolean validateIsEnableEmail(String token) throws JsonProcessingException {

        AuthResponseDto userDto = jwtService.getUserDto(token);

        if (userDto.getIsEnable()) {
            return true;
        }

        return false;
    }

}
