package com.cgr.base.config.security;

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

import com.cgr.base.common.exception.component.AccessDeniedHandlerException;
import com.cgr.base.config.jwt.JwtAuthFilter;

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
                    auth.requestMatchers("/auth/**", "/api/v1/auth/**", "/auth**", "/swagger-ui/**", "/v3/api-docs/**",
                            "/ws-endpoint", "/ws-endpoint/**",
                            "/api/v1/access/module/**").permitAll();
                    auth.requestMatchers("/api/v1/comments/**").hasAuthority("MENU_CERTIFY"); // Cambiado a MENU_CERTIFY
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
                config.setAllowedOrigins(Arrays.asList("http://localhost:5173", "http://localhost:4200",
                        "http://localhost:5173/", "http://192.168.0.220/", "http://192.168.0.220/",
                        "http://localhost:8000/", "http://localhost:8000",
                        "http://localhost:48496", "https://665922d5497f3aaadbaaf8b0--melodic-halva-c4b1b1.netlify.app/",
                        "https://665922d5497f3aaadbaaf8b0--melodic-halva-c4b1b1.netlify.app",
                        "https://bovid.site/", "https://bovid.site", "http://bovid.site/",
                        "http://bovid.site", "https://strong-toffee-1046b5.netlify.app/",
                        "https://strong-toffee-1046b5.netlify.app", "http://192.168.2.63:8001/",
                        "http://192.168.2.63:8001", "http://192.168.27.112:8001/", "http://192.168.27.112:8001"));
                config.setAllowedMethods(Arrays.asList("GET", "POST", "DELETE", "PUT", "OPTIONS"));
                config.setAllowedHeaders(Arrays.asList("*"));
                config.setAllowCredentials(true);
                
                // Tiempo de cache para preflight (1 hora)
                config.setMaxAge(3600L);
                
                return config;
            }
        };
    }
}
