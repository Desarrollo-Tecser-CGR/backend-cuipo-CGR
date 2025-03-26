package com.cgr.base.infrastructure.config;

import java.util.*;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;

import com.cgr.base.infrastructure.exception.component.AccessDeniedHandlerException;
import com.cgr.base.infrastructure.security.Jwt.filters.JwtAuthFilter;
import com.cgr.base.infrastructure.security.endpoints.endpointEntity;
import com.cgr.base.infrastructure.security.endpoints.endpointRepo;
import com.cgr.base.infrastructure.security.endpoints.endpointsSecurity;

import jakarta.servlet.http.HttpServletRequest;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
@EnableWebSecurity
public class SecurityConfig {

    private final AccessDeniedHandlerException accessDeniedHandlerException;
    private final JwtAuthFilter jwtAuthFilter;
    private final endpointRepo endpointRepo;
    private final endpointsSecurity endpointSegurity;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        http.sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        http.csrf(csrf -> csrf.disable())
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                .exceptionHandling(t -> t.accessDeniedHandler(accessDeniedHandlerException))
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class)
                .authorizeHttpRequests(auth -> {
                    List<endpointEntity> endpoints = endpointRepo.findAll();
                    Map<String, Set<String>> restrictedEndpoints = endpointSegurity.getEndpointsWithRoles();

                    for (endpointEntity endpoint : endpoints) {
                        switch (endpoint.getType()) {
                            case "PUBLICO" -> auth.requestMatchers(endpoint.getUrl()).permitAll();
                            case "GENERAL" -> auth.requestMatchers(endpoint.getUrl()).authenticated();
                            case "RESTRINGIDO" -> {
                                Set<String> roles = restrictedEndpoints.get(endpoint.getUrl());
                                if (roles == null || roles.isEmpty()) {
                                    auth.requestMatchers(endpoint.getUrl()).denyAll();
                                } else {
                                    auth.requestMatchers(endpoint.getUrl())
                                            .hasAnyAuthority(roles.toArray(String[]::new));
                                }

                            }
                        }
                    }
                    auth.anyRequest().denyAll();
                    // auth.anyRequest().authenticated();
                });

        http.headers(headers -> headers
                .httpStrictTransportSecurity(hsts -> hsts
                        .includeSubDomains(true)
                        .maxAgeInSeconds(31536000)));

        return http.build();
    }

    // Configuración de CORS
    private CorsConfigurationSource corsConfigurationSource() {
        return new CorsConfigurationSource() {
            @Override
            public CorsConfiguration getCorsConfiguration(@NonNull HttpServletRequest request) {
                CorsConfiguration config = new CorsConfiguration();
                config.setAllowedOrigins(Arrays.asList("http://localhost:5173", "http://localhost:4200",
                        "http://localhost:5173/", "http://192.168.0.220/", "http://192.168.0.220/",
                        "http://localhost:8000/", "http://localhost:8000",
                        "http://localhost:48496", "https://665922d5497f3aaadbaaf8b0--melodic-halva-c4b1b1.netlify.app/",
                        "https://665922d5497f3aaadbaaf8b0--melodic-halva-c4b1b1.netlify.app",
                        "https://bovid.site/", "https://bovid.site", "http://bovid.site/",
                        "http://bovid.site", "https://strong-toffee-1046b5.netlify.app/",
                        "https://strong-toffee-1046b5.netlify.app", "http://192.168.2.63:8001/",
                        "http://192.168.2.63:8001"));
                config.setAllowedMethods(Arrays.asList("GET", "POST", "DELETE", "PUT", "OPTIONS"));
                config.setAllowedHeaders(Arrays.asList("*"));
                config.setAllowCredentials(true);
                config.setExposedHeaders(Arrays.asList("Authorization", "Content-Disposition"));
                config.setMaxAge(3600L);
                return config;
            }
        };
    }
}
