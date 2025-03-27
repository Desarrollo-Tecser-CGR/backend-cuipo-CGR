package com.cgr.base.infrastructure.persistence.entity.access;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "roles_submenu")
@IdClass(SubmenuRole.SubmenuRoleId.class)
@Data
@NoArgsConstructor

public class SubmenuRole {

    @Id
    @Column(name = "role_id")
    private Long roleId;

    @Id
    @Column(name = "submenu_id")
    private Long submenuId;

    @Data
    @NoArgsConstructor
    public static class SubmenuRoleId implements Serializable {
        private Long roleId;
        private Long submenuId;
    }

}
