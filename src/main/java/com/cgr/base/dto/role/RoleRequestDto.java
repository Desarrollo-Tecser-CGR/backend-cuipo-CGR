package com.cgr.base.dto.role;

import lombok.Data;

@Data
public class RoleRequestDto {

    private Long id;
    private String name;
    private boolean enable;
    private String description;

}
