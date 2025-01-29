package com.cgr.base.application.user.service;
import java.util.Base64;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.user.ProfileEntity;
import com.cgr.base.infrastructure.persistence.repository.user.ProfileRepo;

@Service
public class UserProfile {

    @Autowired
    private ProfileRepo userProfileRepo;

    private static final int MAX_IMAGE_SIZE = 2 * 1024 * 1024;  // 2MB
    private static final String[] ALLOWED_FORMATS = {"image/png", "image/jpeg", "image/svg+xml"};

    public ProfileEntity uploadProfileImage(Long userId, String base64Image) {

        byte[] decodedBytes = Base64.getDecoder().decode(base64Image.split(",")[1]);
        if (decodedBytes.length > MAX_IMAGE_SIZE) {
            throw new IllegalArgumentException("Image size exceeds the 2MB limit.");
        }

        String imageType = extractImageType(base64Image);

        boolean isValidFormat = false;
        for (String format : ALLOWED_FORMATS) {
            if (imageType.equals(format)) {
                isValidFormat = true;
                break;
            }
        }
        if (!isValidFormat) {
            throw new IllegalArgumentException("Invalid image format. Only PNG, JPG, and SVG are allowed.");
        }

        Optional<ProfileEntity> existingProfile = userProfileRepo.findById(userId);
        ProfileEntity profile = existingProfile.orElse(new ProfileEntity());
        profile.setUserId(userId);
        profile.setImageProfile(base64Image);

        return userProfileRepo.save(profile);
    }

    // Extraer el tipo MIME de la cadena Base64.
    private String extractImageType(String base64Image) {
        String regex = "data:image/([a-zA-Z]*);base64,";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(base64Image);
        if (matcher.find()) {
            return "image/" + matcher.group(1);
        } else {
            throw new IllegalArgumentException("Invalid Base64 image format.");
        }
    }
}
