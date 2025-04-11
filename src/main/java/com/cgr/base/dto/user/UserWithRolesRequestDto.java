package com.cgr.base.dto.user;

import java.util.List;

import jakarta.validation.constraints.NotNull;
import lombok.Data;

@Data
public class UserWithRolesRequestDto {

    @NotNull
    private Long idUser;

    @NotNull
    private List<Long> roleIds;
}
