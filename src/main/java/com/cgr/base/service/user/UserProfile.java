package com.cgr.base.service.user;

import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.cgr.base.dto.user.UserProfileDto;
import com.cgr.base.entity.user.ProfileEntity;
import com.cgr.base.entity.user.UserEntity;
import com.cgr.base.repository.user.IUserRepositoryJpa;
import com.cgr.base.repository.user.ProfileRepo;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;

@Service
public class UserProfile {

    @Autowired
    private ProfileRepo userProfileRepo;

    @Autowired
    private IUserRepositoryJpa userRepo;

    @PersistenceContext
    private EntityManager entityManager;

    private static final int MAX_IMAGE_SIZE = 2 * 1024 * 1024;

    private static final String[] ALLOWED_FORMATS = { "image/png", "image/jpeg", "image/svg+xml" };

    @Transactional
    public void uploadProfileImage(Long userId, String base64Image) {
        createTableIfNotExists();

        byte[] decodedBytes = Base64.getDecoder().decode(base64Image.split(",")[1]);
        if (decodedBytes.length > MAX_IMAGE_SIZE) {
            throw new IllegalArgumentException("Image size exceeds the limit.");
        }

        String imageType = extractImageType(base64Image);
        if (!Arrays.asList(ALLOWED_FORMATS).contains(imageType)) {
            throw new IllegalArgumentException("Invalid image format. Only PNG, JPG, and SVG are allowed.");
        }

        String sql = """
                    MERGE INTO user_profile AS target
                    USING (SELECT ? AS user_id, ? AS image_profile) AS source
                    ON target.user_id = source.user_id
                    WHEN MATCHED THEN
                        UPDATE SET image_profile = source.image_profile
                    WHEN NOT MATCHED THEN
                        INSERT (user_id, image_profile)
                        VALUES (source.user_id, source.image_profile);
                """;

        entityManager.createNativeQuery(sql)
                .setParameter(1, userId)
                .setParameter(2, base64Image)
                .executeUpdate();
    }

    @Transactional
    public void createTableIfNotExists() {
        String checkTableQuery = """
                    IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE UPPER(TABLE_NAME) = 'USER_PROFILE')
                    SELECT 1 ELSE SELECT 0;
                """;

        Number tableExists = (Number) entityManager.createNativeQuery(checkTableQuery).getSingleResult();

        if (tableExists.intValue() == 0) {
            String createTableSQL = """
                        CREATE TABLE user_profile (
                            user_id BIGINT PRIMARY KEY,
                            image_profile NVARCHAR(MAX) NULL
                        );
                    """;
            entityManager.createNativeQuery(createTableSQL).executeUpdate();
        }
    }

    // Método para obtener la imagen de perfil en base64
    public String getProfileImage(Long userId) {
        Optional<ProfileEntity> profile = userProfileRepo.findById(userId);
        if (profile.isEmpty() || profile.get().getImageProfile() == null) {
            throw new IllegalArgumentException("Profile Image Not Found.");
        }
        return profile.get().getImageProfile();
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

    // Actualizar Información de Perfil del Usuario.
    @Transactional
    public UserEntity updateUserProfile(Long userId, UserProfileDto userDto) {
        Optional<UserEntity> optionalUser = userRepo.findById(userId);

        if (optionalUser.isEmpty()) {
            throw new IllegalArgumentException("User not found.");
        }

        UserEntity user = optionalUser.get();

        if (userDto.getEmail() != null) {
            user.setEmail(userDto.getEmail());
        }

        if (userDto.getFullName() != null) {
            user.setFullName(userDto.getFullName());
        }

        if (userDto.getPhone() != null) {
            user.setPhone(userDto.getPhone());
        }

        user.setDateModify(new Date());
        return userRepo.save(user);
    }

    public Map<String, Object> getUserById(Long userId) {
        UserEntity user = userRepo.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("User not found."));

        Map<String, Object> userData = new HashMap<>();
        userData.put("usuario", user.getSAMAccountName());
        userData.put("cargo", user.getCargo());
        userData.put("email", user.getEmail());
        userData.put("nombre", user.getFullName());
        userData.put("telefono", user.getPhone());
        return userData;
    }

}