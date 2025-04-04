package com.cgr.base.domain.dto.dtoRoleMenu;

import jakarta.persistence.Column;
import lombok.Data;

@Data
public class DtoRoleMenu {

    private Integer id;
    private Integer role_id;
    private Integer submenu_id;
    private String name_rol;
    private String Submenu_title;
    private String Link;
}
