package com.cgr.base.application.maps.servicesMaps;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.io.File;

@Service
public class ServiceImage {

    private static final String IMAGE_FOLDER = "static/imagenesMaps/"; // Ruta dentro del classpath

    @Value("${server.servlet.context-path:/}")
    private String contextPath;

    @Value("${server.port:8080}")
    private String serverPort;

    @Value("${server.address:localhost}")
    private String serverAddress;

    public String getImageUrlForDepartment(long id, String dptoNombre, String ccdgo) {
        String imageName;

        // Mapear ccdgo a nombre de imagen
        switch (ccdgo) {
            case "44":
                imageName = "La_Guajira.png";
                break;
            case "44430":
                imageName = "Maicao.png";
                break;
            case "44560":
                imageName = "Manaure.png";
                break;
            case "44867":
                imageName = "Uribia.png";
                break;
            case "44001":
                imageName = "Rioacaha.png";
                break;
            default:
                imageName = "default.png"; // Imagen por defecto
                break;
        }

        // Verificar si el archivo existe en el backend
        Resource resource = new ClassPathResource(IMAGE_FOLDER + imageName);
        File file;
        try {
            file = resource.getFile();
            if (file.exists()) {
                // Construir una URL absoluta basada en el contexto de la aplicación
                return ServletUriComponentsBuilder.fromCurrentContextPath()
                        .path("/imagenesMaps/") // Ajustado para coincidir con la carpeta real
                        .path(imageName)
                        .toUriString();
            }
        } catch (Exception e) {
            System.out.println("Error al buscar imagen " + imageName + ": " + e.getMessage());
        }

        // Retornar URL de la imagen por defecto si no se encuentra
        return ServletUriComponentsBuilder.fromCurrentContextPath()
                .path("/imagenesMaps/default.png")
                .toUriString();
    }
}