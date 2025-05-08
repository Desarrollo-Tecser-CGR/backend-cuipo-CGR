package com.cgr.base.dto.role;

import lombok.Data;

@Data
public class RoleDto {

    private String name;

    public RoleDto(String name) {
        this.name = name;
    }

}
