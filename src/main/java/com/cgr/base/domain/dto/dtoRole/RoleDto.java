package com.cgr.base.domain.dto.dtoRole;

import lombok.Data;

@Data
public class RoleDto {

    private String name;

    public RoleDto(String name) {
        this.name = name;
    }

}
