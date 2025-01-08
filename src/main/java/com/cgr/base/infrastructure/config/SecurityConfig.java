package com.cgr.base.infrastructure.config;

import java.util.Arrays;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;

import com.cgr.base.infrastructure.exception.component.AccessDeniedHandlerException;
import com.cgr.base.infrastructure.security.Jwt.filters.JwtAuthFilter;
import com.cgr.base.infrastructure.security.helper.RoleUtil;

import lombok.RequiredArgsConstructor;

@Configuration
@RequiredArgsConstructor
@EnableWebSecurity
public class SecurityConfig {

    private final AccessDeniedHandlerException accessDeniedHandlerException;
    private final JwtAuthFilter jwtAuthFilter;

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        // RequestMatcher authMatcher = new MvcRequestMatcher(new
        // HandlerMappingIntrospector(), "/auth/**");

        http.sessionManagement(
                session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        http.csrf(csrf -> csrf.disable())
                .cors(cors -> cors.configurationSource(corsConfigurationSource()))
                .exceptionHandling(t -> t.accessDeniedHandler(accessDeniedHandlerException))
                .addFilterBefore(jwtAuthFilter, UsernamePasswordAuthenticationFilter.class)

                .authorizeHttpRequests(auth -> {
                    auth.requestMatchers("/auth/**", "/api/v1/auth/**", "/auth**").permitAll();
                    auth.requestMatchers("/test").permitAll();
                    auth.requestMatchers("/api/v1/menu/**").permitAll();
                    auth.requestMatchers("/swagger-ui/**").permitAll();
                    auth.requestMatchers("/v3/api-docs/**").permitAll();
                    auth.requestMatchers("/api/v1/log/**").permitAll();
                    auth.requestMatchers("/api/v1/role/**").permitAll();
                    auth.requestMatchers("/api/v1/user/**").permitAll();
                    auth.requestMatchers("/api/csv/**").permitAll();
                    auth.requestMatchers("/api/rules").permitAll();
                    auth.requestMatchers("/api/excel").permitAll();
                    auth.requestMatchers("/EmailEnviar").permitAll();
                    auth.requestMatchers("/admin/**").hasAnyRole(RoleUtil.ADMIN, RoleUtil.FUNCIONARIO,
                            RoleUtil.Usuario);
                    auth.requestMatchers("/user/**").hasAnyRole(RoleUtil.FUNCIONARIO,
                            RoleUtil.ADMIN, RoleUtil.Usuario);
                    auth.anyRequest().authenticated();
                });

        http.headers(headers -> headers
                .httpStrictTransportSecurity(hsts -> hsts
                        .includeSubDomains(true)
                        .maxAgeInSeconds(31536000)));

        return http.build();
    }

    private CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration config = new CorsConfiguration();
        config.setAllowedOriginPatterns(Arrays.asList(
                "http://localhost:4200",
                "http://127.0.0.1:5500/"));
        config.setAllowedMethods(Arrays.asList("GET", "POST", "DELETE", "PUT", "OPTIONS"));
        config.setAllowedHeaders(Arrays.asList("*"));
        config.setAllowCredentials(true); // Habilitar credenciales
        config.setExposedHeaders(Arrays.asList("Authorization", "Content-Disposition"));
        config.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", config);
        return source;
    }
}

