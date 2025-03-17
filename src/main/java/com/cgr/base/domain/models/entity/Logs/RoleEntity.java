package com.cgr.base.domain.models.entity.Logs;

import java.util.Date;
import java.util.List;

import com.cgr.base.application.services.role.service.permission.EntityPermission;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@Table(name = "roles")
public class RoleEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_role")
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

    @JsonIgnoreProperties({ "roles", "handler", "hibernateLazyInitializer" })
    @ManyToMany(mappedBy = "roles")
    private List<UserEntity> users;

    @ManyToMany
    @JsonIgnoreProperties({ "roles", "handler", "hibernateLazyInitializer" })
    @JsonIgnore
    @JoinTable(
            name = "roles_submenu",
            joinColumns = @JoinColumn(name = "role_id"),
            inverseJoinColumns = @JoinColumn(name = "submenu_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = { "role_id", "submenu_id" })
    )
    private List<SubMenuEntity> subMenus;

    // Relación entre roles y permisos
    @ManyToMany
    @JsonIgnore
    @JoinTable(
            name = "roles_permisos",
            joinColumns = @JoinColumn(name = "id_role"), // Debe coincidir con el nombre en la BD
            inverseJoinColumns = @JoinColumn(name = "permiso_id"),
            uniqueConstraints = @UniqueConstraint(columnNames = { "id_role", "permiso_id" })
    )
    private List<EntityPermission> permisos;
}
