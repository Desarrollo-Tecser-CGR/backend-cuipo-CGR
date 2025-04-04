package com.cgr.base.domain.models.entity.Logs.exit;

import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.management.relation.Role;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Entity
@Table(name = "roles_submenu")
public class DinamicRoleSubmenu {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @ManyToOne
    @JoinColumn(name = "role_id", referencedColumnName = "id_role", insertable = false, updatable = false)
    private RoleEntity role;  // Relación con RoleEntity

    @ManyToOne
    @JoinColumn(name = "submenu_id", referencedColumnName = "id", insertable = false, updatable = false)
    private SubMenuEntity submenu;  // Relación con SubMenuEntity

    @Column(name = "role_id")
    private Integer role_id;  // Este campo es redundante para consultas, pero necesario si no quieres modificar tu base de datos.

    @Column(name = "submenu_id")
    private Integer submenu_id;
}