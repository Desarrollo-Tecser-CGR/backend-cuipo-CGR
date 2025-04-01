package com.cgr.base.infrastructure.security.interceptor;

import java.io.IOException;
import java.util.Enumeration;

import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class HeadersInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        try {
            // Validar Content-Type
            String contentType = request.getHeader("Content-Type");
            if (contentType == null || !contentType.equals("application/json")) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\": \"Invalid Content-Type, expected application/json\"}");
                response.getWriter().flush();
                return false;
            }

            // Validar Host
            String host = request.getHeader("Host");
            if (host == null || host.isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\": \"Host header is required\"}");
                response.getWriter().flush();
                return false;
            }

            // Validar Accept (Opcional, pero recomendado)
            String accept = request.getHeader("Accept");
            if (accept == null) {
                response.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\": \"Invalid Accept header, expected application/json\"}");
                response.getWriter().flush();
                return false;
            }

            // Validar Origin (Opcional, útil para CORS)
            // String origin = request.getHeader("Origin");
            // if (origin != null && !origin.matches("https?://(.*)")) { // Validar que sea
            // un dominio válido
            // response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            // response.setContentType("application/json");
            // response.getWriter().write("{\"error\": \"Invalid Origin\"}");
            // response.getWriter().flush();
            // return false;
            // }

            // Validar User-Agent (Opcional, útil para auditoría y seguridad)
            String userAgent = request.getHeader("User-Agent");
            if (userAgent == null || userAgent.isEmpty()) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                response.setContentType("application/json");
                response.getWriter().write("{\"error\": \"User-Agent header is required\"}");
                response.getWriter().flush();
                return false;
            }

        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true; // Continuar con la petición si pasa todas las validaciones
    }
}
