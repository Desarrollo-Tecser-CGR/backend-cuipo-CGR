package com.cgr.base.entity.user;

import java.util.Date;
import java.util.List;

import com.cgr.base.entity.role.RoleEntity;
import com.fasterxml.jackson.annotation.JsonFormat;
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
import lombok.Data;

@Data
@Entity
@Table(name = "users")
public class UserEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "sAMAccountName")
    private String sAMAccountName;

    @Column(name = "enabled")
    private Boolean enabled;

    @Column(name = "full_name")
    private String fullName;

    @Column(name = "email")
    private String email;

    @Column(name = "phone")
    private String phone;

    @Column(name = "cargo")
    private String cargo;

    @Column(name = "date_modify")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "UTC")
    private Date dateModify;

    @ManyToMany
    @JsonIgnoreProperties({ "users", "handler", "hibernateLazyInitializer" })
    @JoinTable(name = "users_roles", joinColumns = @JoinColumn(name = "user_id"), inverseJoinColumns = @JoinColumn(name = "role_id"), uniqueConstraints = {
            @UniqueConstraint(columnNames = { "user_id", "role_id" }) })
    private List<RoleEntity> roles;

    public void addRol(RoleEntity roleEntity) {
        this.roles.add(roleEntity);
        roleEntity.getUsers().add(this);
    }

    public void mapActiveDirectoryUser(UserEntity userAD) {
        if (this.fullName == null) {
            this.fullName = userAD.getFullName();
        }
        if (this.email == null) {
            this.email = userAD.getEmail();
        }
        if (this.phone == null) {
            this.phone = userAD.getPhone();
        }
        if (this.dateModify == null) {
            this.dateModify = userAD.getDateModify();
        }
        this.cargo = userAD.getCargo();
        this.enabled = userAD.getEnabled();
    }

}
