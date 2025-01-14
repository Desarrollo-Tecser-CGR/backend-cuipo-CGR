package com.cgr.base.application.auth.dto;

import java.util.List;

import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserAuthDto {
    @NotEmpty
    @JsonProperty("sAMAccountName")
     private String sAMAccountName;
  

}


