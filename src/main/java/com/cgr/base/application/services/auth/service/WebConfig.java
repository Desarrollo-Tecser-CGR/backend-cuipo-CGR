package com.cgr.base.application.services.auth.service;


import com.cgr.base.infrastructure.security.interceptor.HeadersInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Autowired
    private HeadersInterceptor headersInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(headersInterceptor)
                .addPathPatterns("/api/v1/notification"); // Aplica el interceptor a esta ruta
        // .addPathPatterns("/**"); // Aplica el interceptor a todas las rutas
    }
}