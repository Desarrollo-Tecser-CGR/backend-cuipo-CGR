package com.cgr.base.domain.dto.dtoUser;

import java.util.List;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class UserWithRolesRequestDto {

    @NotNull
    private Long idUser;

    private String roles;
    @NotNull
    private List<Long> roleIds;
}
