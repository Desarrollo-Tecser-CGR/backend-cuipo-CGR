package com.cgr.base.controller.user;

import java.io.IOException;
import java.util.*;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import com.cgr.base.config.abstractResponse.AbstractController;
import com.cgr.base.config.jwt.JwtService;
import com.cgr.base.dto.user.UserProfileDto;
import com.cgr.base.service.user.UserProfile;

import jakarta.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/api/v1/user/profile")
public class ProfileController extends AbstractController {

    @Autowired
    private UserProfile profileService;

    @Autowired
    private JwtService jwtService;

    private String convertToBase64(MultipartFile image) throws IOException {
        byte[] bytes = image.getBytes();
        return "data:" + image.getContentType() + ";base64," + Base64.getEncoder().encodeToString(bytes);
    }

    private String getToken(HttpServletRequest request) {
        String header = request.getHeader(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith("Bearer ")) {
            throw new SecurityException("Token is Required."); // Ensure SecurityException is thrown
        }
        return header.split(" ")[1];
    }

    private Long getUserIdFromRequest(HttpServletRequest request) {
        String token = getToken(request);
        Long userId = jwtService.extractUserIdFromToken(token);
        if (userId == null) {
            throw new SecurityException("User ID not Found."); // Ensure SecurityException is thrown
        }
        return userId;
    }

    @PostMapping("/upload_image")
    public ResponseEntity<?> uploadProfileImage(
            @RequestParam("image") MultipartFile image,
            HttpServletRequest request) throws IOException {

        Long userId = getUserIdFromRequest(request);
        String base64Image = convertToBase64(image);

        profileService.uploadProfileImage(userId, base64Image);

        return requestResponse(null, "Profile Image Uploaded Successfully.", HttpStatus.OK, true);
    }

    @GetMapping("/image")
    public ResponseEntity<?> getProfileImage(HttpServletRequest request) {
        Long userId = getUserIdFromRequest(request);
        String base64Image = profileService.getProfileImage(userId);

        return requestResponse(base64Image, "Image Retrieved Successfully.", HttpStatus.OK, true);
    }

    @PostMapping("/update")
    public ResponseEntity<?> updateUserProfile(
            @RequestBody UserProfileDto userDto,
            HttpServletRequest request) {

        Long userId = getUserIdFromRequest(request);
        Map<String, Object> updatedUser = profileService.updateUserProfile(userId, userDto);
        return requestResponse(updatedUser, "User Profile Updated.", HttpStatus.OK, true);
    }

    @GetMapping("/me")
    public ResponseEntity<?> getUserInfo(HttpServletRequest request) {
        Long userId = getUserIdFromRequest(request);
        Object userData = profileService.getUserById(userId);

        return requestResponse(userData, "User Data Retrieved.", HttpStatus.OK, true);
    }
}
