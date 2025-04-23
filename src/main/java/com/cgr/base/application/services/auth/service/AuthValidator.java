package com.cgr.base.application.services.auth.service;

import com.cgr.base.domain.dto.dtoAuth.AuthRequestDto;
import com.cgr.base.domain.exception.customException.MessageException;
import io.micrometer.common.util.StringUtils;
import org.springframework.http.HttpStatus;

public class AuthValidator {

    public static void validateLoginRequest(AuthRequestDto userRequest) {
        if (userRequest == null ||
                StringUtils.isBlank(userRequest.getSAMAccountName()) ||
                StringUtils.isBlank(userRequest.getPassword())) {
            throw new MessageException(HttpStatus.BAD_REQUEST + " Por favor rellene todos los campos");
        }
    }

   public static void validatePasswordFormat(String password) {
        if (StringUtils.isBlank(password)) {
            return; // Permitir contraseña vacía si la validación de campos obligatorios se maneja en otro lugar
        }

        final int MIN_LENGTH = 8;
        final boolean REQUIRE_UPPERCASE = true;
        final boolean REQUIRE_LOWERCASE = true;
        final boolean REQUIRE_DIGIT = true;
        final boolean REQUIRE_SPECIAL_CHAR = true;
        final String SPECIAL_CHARACTERS = "!@#$%^&*()-_=+[]{}|;:'\",.<>/?`~";

        if (password.length() < MIN_LENGTH) {
            throw new MessageException("La contraseña debe tener al menos " + MIN_LENGTH + " caracteres.");
        }

        if (REQUIRE_UPPERCASE && !password.matches(".*[A-Z].*")) {
            throw new MessageException("La contraseña debe contener al menos una letra mayúscula.");
        }

        if (REQUIRE_LOWERCASE && !password.matches(".*[a-z].*")) {
            throw new MessageException("La contraseña debe contener al menos una letra minúscula.");
        }

        if (REQUIRE_DIGIT && !password.matches(".*\\d.*")) {
            throw new MessageException("La contraseña debe contener al menos un dígito.");
        }

        if (REQUIRE_SPECIAL_CHAR) {
            boolean hasSpecial = false;
            for (char c : password.toCharArray()) {
                if (SPECIAL_CHARACTERS.contains(String.valueOf(c))) {
                    hasSpecial = true;
                    break;
                }
            }
            if (!hasSpecial) {
                throw new MessageException("La contraseña debe contener al menos un carácter especial (" + SPECIAL_CHARACTERS + ").");
            }
        }
    }
}
