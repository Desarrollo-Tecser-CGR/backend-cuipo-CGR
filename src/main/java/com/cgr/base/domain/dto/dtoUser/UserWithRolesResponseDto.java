package com.cgr.base.domain.dto.dtoUser;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cgr.base.domain.dto.dtoLogs.logsIngress.LogDto;
import com.cgr.base.domain.dto.dtoRole.RoleRequestDto;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;

import lombok.Data;

@Data
public class UserWithRolesResponseDto {

    private Long idUser;

    private String userName;

    private String fullName;

    private String email;

    private String phone;

    private boolean enabled;

    private Date dateModify;

    private String cargo;

    private List<RoleRequestDto> roles = new ArrayList<>();

    private List<LogDto> logs = new ArrayList<>();

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

    private String userType;
}
