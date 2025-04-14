package com.cgr.base.controller.user;

import java.io.IOException;
import java.util.Base64;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.dto.user.UserProfileDto;
import com.cgr.base.service.user.UserProfile;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/user/profile")
public class ProfileController {

    @Autowired
    private UserProfile ProfileService;

    @Autowired
    private JwtService jwtService;

    private String convertToBase64(MultipartFile image) throws IOException {
        byte[] bytes = image.getBytes();
        return "data:" + image.getContentType() + ";base64," + Base64.getEncoder().encodeToString(bytes);
    }

    @PostMapping("/upload_image")
    public ResponseEntity<String> uploadProfileImage(
            @RequestParam("image") MultipartFile image,
            HttpServletRequest request) throws IOException {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            return new ResponseEntity<>("User ID not Found.", HttpStatus.FORBIDDEN);
        }

        String base64Image = convertToBase64(image);
        try {

            ProfileService.uploadProfileImage(userId, base64Image);
            return new ResponseEntity<>("Profile Image Uploaded Successfully.", HttpStatus.OK);

        } catch (IllegalArgumentException e) {

            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/image")
    public ResponseEntity<String> getProfileImage(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return new ResponseEntity<>("Token is Required.", HttpStatus.FORBIDDEN);
        }

        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            return new ResponseEntity<>("User ID not Found.", HttpStatus.FORBIDDEN);
        }

        try {
            String base64Image = ProfileService.getProfileImage(userId);
            return new ResponseEntity<>(base64Image, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.NOT_FOUND);
        }
    }

    @PostMapping("/update")
    public ResponseEntity<String> updateUserProfile(
            @RequestBody UserProfileDto userDto,
            HttpServletRequest request) {

        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        String token = header.split(" ")[1];

        Long userId = jwtService.extractUserIdFromToken(token);

        if (userId == null) {
            return new ResponseEntity<>("User ID not Found.", HttpStatus.FORBIDDEN);
        }

        try {

            ProfileService.updateUserProfile(userId, userDto);
            return new ResponseEntity<>("User Profile Updated.", HttpStatus.OK);

        } catch (IllegalArgumentException e) {

            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    @GetMapping("/me")
    public ResponseEntity<Object> getUserInfo(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            return new ResponseEntity<>("Token is Required.", HttpStatus.FORBIDDEN);
        }

        String token = header.split(" ")[1];
        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            return new ResponseEntity<>("User ID not Found.", HttpStatus.FORBIDDEN);
        }

        try {
            Object userData = ProfileService.getUserById(userId);
            return new ResponseEntity<>(userData, HttpStatus.OK);
        } catch (IllegalArgumentException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.NOT_FOUND);
        }
    }

}