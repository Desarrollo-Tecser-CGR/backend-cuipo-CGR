package com.cgr.base.infrastructure.externar.repository;

import org.springframework.stereotype.Component;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.HttpHeaders;
//import org.springframework.http.HttpEntity;
import org.springframework.http.ResponseEntity;

import com.cgr.base.application.auth.service.ExternalAuthService;
import com.cgr.base.domain.repository.IActiveDirectoryUserRepository;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class LDAPUsuarioRepository implements IActiveDirectoryUserRepository {

    @Autowired
    private ExternalAuthService externalAuthService; // Inyección del servicio de autenticación externo

    private final RestTemplate restTemplate = new RestTemplate();

    private String externalUserInfoUrl = "https://serviciosint.contraloria.gov.co/directorio/usuarios/consultar/";

    @Override
    public Boolean checkAccount(String samAccountName, String password) {
        try {
            // Usar el servicio ExternalAuthService para autenticar al usuario
            boolean isAuthenticated = externalAuthService.authenticateWithExternalService(samAccountName, password);
            if (!isAuthenticated) {
                throw new Exception("Invalid credentials for user: " + samAccountName);
            }

            // Obtener información adicional del usuario desde el endpoint externo
            UserEntity userEntity = obtenerInformacionUsuarioExterna(samAccountName);
            if (userEntity != null) {
                System.out.println("Información del usuario obtenida del endpoint externo: " + userEntity);
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    private UserEntity obtenerInformacionUsuarioExterna(String samAccountName) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("usuario", samAccountName);

            // HttpEntity<Void> requestEntity = new HttpEntity<>(headers);

            // Realizar la solicitud GET al endpoint externo
            ResponseEntity<String> response = restTemplate.getForEntity(externalUserInfoUrl + samAccountName,
                    String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                // Parsear la respuesta JSON
                ObjectMapper objectMapper = new ObjectMapper();
                JsonNode jsonNode = objectMapper.readTree(response.getBody());

                // Mapear la información al objeto UserEntity
                UserEntity userEntity = new UserEntity();
                userEntity.setSAMAccountName(jsonNode.get("usuario").asText());
                userEntity.setFullName(jsonNode.get("nombreMostrar").asText());
                userEntity.setEmail(jsonNode.get("usuario").asText() + "@contraloria.gov.co");
                userEntity.setPhone(jsonNode.get("numeroTelefono").asText());
                userEntity.setCargo(jsonNode.get("descripcionCargo").asText());
                userEntity.setEnabled(true);

                return userEntity;
            } else {
                System.err.println("Error al obtener información del usuario desde el endpoint externo.");
                return null;
            }
        } catch (Exception e) {
            return null;
        }
    }

    @Override
    public List<UserEntity> getAllUsers() {
        throw new UnsupportedOperationException("Operación no soportada en este repositorio.");
    }
}