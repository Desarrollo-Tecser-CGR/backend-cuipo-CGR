package com.cgr.base.entity.access;

import java.io.Serializable;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "menu_roles")
@IdClass(MenuRole.MenuRoleId.class)
@Data
@NoArgsConstructor
public class MenuRole {

    @Id
    @Column(name = "role_id")
    private Long roleId;

    @Id
    @Column(name = "menu_id")
    private Long menuId;

    @Data
    @NoArgsConstructor
    public static class MenuRoleId implements Serializable {
        private Long roleId;
        private Long menuId;
    }

}
