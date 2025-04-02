package com.cgr.base.application.auth.service;

import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@Service
public class ExternalAuthService {

    @Value("${external.auth.url}")
    private String externalAuthUrl;

    public boolean authenticateWithExternalService(String username, String password) {

        System.out.println(username + " " + password);

        String requestBody = String.format("{\"Username\": \"%s\", \"Password\": \"%s\"}", username, password);

        try {

            HttpClient httpClient = HttpClient.newHttpClient();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(externalAuthUrl)) // URL del servicio
                    .header("Content-Type", "application/json") // Cabecera para JSON
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody)) // Cuerpo de la petición
                    .build();

            // Enviar la solicitud y obtener la respuesta
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            // Imprimir el código de estado y el cuerpo de la respuesta
            System.out.println("Código de estado: " + response.statusCode());
            System.out.println("Respuesta: " + response.body());

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(response.body());

            // Extraer el valor de la clave "respuesta"
            Boolean respuesta = jsonNode.get("resultado").asBoolean();

            System.out.println("Valor de 'respuesta': " + respuesta);

            return respuesta;
        } catch (Exception e) {

            e.printStackTrace();

            return false;
        }
    }
}