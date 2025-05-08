package com.cgr.base.infrastructure.config;

import java.util.Arrays;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;

import com.cgr.base.infrastructure.exception.component.AccessDeniedHandlerException;
import com.cgr.base.infrastructure.security.Jwt.filters.JwtAuthFilter;

import jakarta.servlet.http.HttpServletRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {

    private final AccessDeniedHandlerException accessDeniedHandlerException;
    private final JwtAuthFilter jwtAuthFilter;

    // Configuración de la cadena de filtros de seguridad.
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        http.csrf(csrf -> csrf.disable())
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                .exceptionHandling(t -> t.accessDeniedHandler(accessDeniedHandlerException))
                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers("/auth/**", "/api/v1/auth/**", "/auth**", "/swagger-ui/**", "/v3/api-docs/**", "/ws-endpoint", "/ws-endpoint/**",
                            "/api/v1/access/module/**").permitAll();
                    auth.anyRequest().authenticated();
                }).sessionManagement(sess -> sess.sessionCreationPolicy(SessionCreationPolicy.STATELESS)).addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class);

        http.headers(headers -> headers
                .httpStrictTransportSecurity(hsts -> hsts
                        .includeSubDomains(true)
                        .maxAgeInSeconds(31536000)));

        return http.build();
    }

    private CorsConfigurationSource corsConfigurationSource() {
        return new CorsConfigurationSource() {
            @Override
            public CorsConfiguration getCorsConfiguration(@NonNull HttpServletRequest request) {
                CorsConfiguration config = new CorsConfiguration();
                
                // Lista de orígenes permitidos (añade los que necesites)
                config.setAllowedOrigins(Arrays.asList(
                    "http://localhost:4200",           // Angular dev server
                    "http://127.0.0.1:4200",           // http-server default
                    "http://192.168.2.64:4200",        // Si accedes por IP local
                    "http://localhost:5173",           // Vite/React
                    "https://tu-app-production.com",    // Dominio en producción
                    "http://192.168.0.220",            // Otras IPs necesarias
                    "http://192.168.2.63:8001"         // Backend local (si aplica)
                ));
    
                // Métodos HTTP permitidos
                config.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"));
                
                // Headers permitidos (usar "*" puede ser inseguro en producción)
                config.setAllowedHeaders(Arrays.asList(
                    "Authorization", 
                    "Content-Type", 
                    "Accept", 
                    "X-Requested-With",
                    "Cache-Control"
                ));
                
                // Headers expuestos en la respuesta
                config.setExposedHeaders(Arrays.asList(
                    "Authorization", 
                    "Content-Disposition"
                ));
                
                // Permitir credenciales (cookies, auth headers)
                config.setAllowCredentials(true);
                
                // Tiempo de cache para preflight (1 hora)
                config.setMaxAge(3600L);
                
                return config;
            }
        };
    }
}
