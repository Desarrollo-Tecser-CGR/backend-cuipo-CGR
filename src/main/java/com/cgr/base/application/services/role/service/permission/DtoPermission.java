package com.cgr.base.application.services.role.service.permission;

import jakarta.persistence.Column;
import lombok.Data;

@Data
public class DtoPermission {

    private Integer id;
    private String name_permission;
    private String description_permission;

}
