package com.cgr.base.dto.user;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cgr.base.application.role.dto.RoleRequestDto;
import com.cgr.base.infrastructure.persistence.entity.role.RoleEntity;
import com.fasterxml.jackson.annotation.JsonFormat;

import lombok.Data;

@Data
public class UserWithRolesResponseDto {

    private Long idUser;

    private String userName;

    private String fullName;

    private String email;

    private String phone;

    private boolean enabled;

    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    private Date dateModify;

    private String cargo;

    private List<RoleRequestDto> roles = new ArrayList<>();

    public void addRole(List<RoleEntity> rolesEntity) {
        rolesEntity.forEach(role -> {
            var role2 = new RoleRequestDto();
            role2.setId(role.getId());
            role2.setName(role.getName());
            role2.setDescription(role.getDescription());
            role2.setEnable(role.isEnable());
            roles.add(role2);
        });

    }
}
