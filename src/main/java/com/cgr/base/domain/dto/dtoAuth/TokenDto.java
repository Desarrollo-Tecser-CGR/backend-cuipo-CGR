package com.cgr.base.domain.dto.dtoAuth;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;


@Data
public class TokenDto {
    @NotBlank
    private String token;

}