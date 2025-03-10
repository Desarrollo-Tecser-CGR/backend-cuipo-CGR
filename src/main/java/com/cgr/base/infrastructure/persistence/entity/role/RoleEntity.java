
package com.cgr.base.infrastructure.persistence.entity.role;

import java.util.Date;
import java.util.Set;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.entity.Menu.SubMenuEntity;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@Table(name = "roles")
public class RoleEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotBlank
    @Column(name = "name", nullable = false, length = 100)
    private String name;

    @Column(name = "enable", nullable = false)
    private boolean enable;

    @NotBlank
    @Column(name = "description", length = 255)
    private String description;

    @CreationTimestamp
    @Column(name = "date_create", updatable = false)
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "America/Bogota")
    private Date dateSesionStart;

    @UpdateTimestamp
    @Column(name = "date_modify")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "America/Bogota")
    private Date dateModify;

    @ManyToMany(mappedBy = "roles")
    @JsonIgnore  // Evitas recursión o lazy initialization
    private Set<UserEntity> users;

    // Relación con SubMenu (tabla roles_submenu)
    @ManyToMany
    @JoinTable(
        name = "roles_submenu",
        joinColumns = @JoinColumn(name = "role_id"),
        inverseJoinColumns = @JoinColumn(name = "submenu_id"),
        uniqueConstraints = { 
            @UniqueConstraint(columnNames = { "role_id", "submenu_id" })
        }
    )
    @JsonIgnore // Si no quieres que en el JSON aparezcan los submenús
    private Set<SubMenuEntity> subMenus;

    // Relación inversa con Menu (tabla menu_roles)
    @ManyToMany(mappedBy = "roles")
    @JsonIgnoreProperties({"roles", "children"})
    private Set<Menu> menus;

}

