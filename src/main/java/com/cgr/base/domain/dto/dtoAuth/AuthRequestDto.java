package com.cgr.base.domain.dto.dtoAuth;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AuthRequestDto {
    
    @JsonProperty("Username")
    private String Username;
    @JsonProperty("Password")
    private String Password;
    private String email;
    private String tipe_of_income;

}
