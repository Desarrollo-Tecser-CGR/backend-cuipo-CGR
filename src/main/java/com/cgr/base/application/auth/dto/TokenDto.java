package com.cgr.base.application.auth.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;


@Data
public class TokenDto {
    @NotBlank
    private String token;

}