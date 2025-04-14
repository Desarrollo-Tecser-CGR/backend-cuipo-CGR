package com.cgr.base.external.CGR;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class ExternalAuthService {

    @Value("${external.auth.url}")
    private String externalAuthUrl;

    public boolean authenticateWithExternalService(String username, String password) {

        String requestBody = String.format("{\"Username\": \"%s\", \"Password\": \"%s\"}", username, password);

        try {

            HttpClient httpClient = HttpClient.newHttpClient();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(externalAuthUrl))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(response.body());

            Boolean respuesta = jsonNode.get("resultado").asBoolean();

            return respuesta;
        } catch (Exception e) {

            e.printStackTrace();

            return false;
        }
    }
    
}
