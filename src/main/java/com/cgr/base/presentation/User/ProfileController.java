package com.cgr.base.presentation.User;

import java.io.IOException;
import java.util.Base64;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.auth0.jwt.JWT;
import com.cgr.base.application.user.service.UserProfile;
import com.cgr.base.infrastructure.persistence.repository.user.IUserRepositoryJpa;
import com.cgr.base.infrastructure.security.Jwt.services.JwtService;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/user")
public class ProfileController {

    @Autowired
    private UserProfile ProfileService;

    @Autowired
    private JwtService jwtService;

    @Autowired
    private IUserRepositoryJpa userRepo;

    private String convertToBase64(MultipartFile image) throws IOException {
        byte[] bytes = image.getBytes();
        return "data:" + image.getContentType() + ";base64," + Base64.getEncoder().encodeToString(bytes);
    }

    @PostMapping("/upload_image")
    public ResponseEntity<String> uploadProfileImage(
            @RequestParam("image") MultipartFile image,
            HttpServletRequest request) throws IOException {

        
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        System.out.println("header" + header);

        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);
        System.out.println("Extracted userId: " + userId);
                // Obtener el token de la solicitud
        

        String userName = JWT.decode(token).getClaim("userName").asString();
        System.out.println("userName: " + userName);

        

        // Validar si el userId es nulo
        if (userId == null) {
            System.out.println("User ID not found in token.");
            return new ResponseEntity<>("User ID not Found.", HttpStatus.FORBIDDEN);
        }

        // Convertir la imagen a Base64
        String base64Image = convertToBase64(image);
        System.out.println("Image converted to Base64 successfully.");

        try {
            // Guardar la imagen en la base de datos
            ProfileService.uploadProfileImage(userId, base64Image);
            System.out.println("Profile image uploaded successfully for user ID: " + userId);
            return new ResponseEntity<>("Profile Image Uploaded Successfully.", HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            System.out.println("Error uploading image: " + e.getMessage());
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

}
