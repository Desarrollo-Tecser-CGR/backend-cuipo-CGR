package com.cgr.base.domain.models.entity.Logs;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.cgr.base.domain.models.entity.EntityNotification;
import com.cgr.base.domain.models.entity.Logs.exit.LogExitEntity;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.JoinTable;
import jakarta.persistence.Lob;
import jakarta.persistence.ManyToMany;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.validation.constraints.NotBlank;
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

    private String password;

    @Column(name = "full_name")
    private String fullName;

    private String email;

    private String phone;

    @Column(name = "enabled")
    private Boolean enabled;

    @Column(name = "date_modify")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone = "America/Bogota")
    private Date dateModify;

    private String cargo;

    @Column(name = "user_type")
    private String userType;

    // jhon
    @Column(name = "failed_attempts")
    private Integer failedAttempts = 0;

    @Column(name = "lock_time")
    private LocalDateTime lockTime;

    @OneToMany(mappedBy = "user")
    private List<LogEntity> logs = new ArrayList<>();

    @OneToMany(mappedBy = "user")
    private List<LogExitEntity> logsExit = new ArrayList<>();

    @OneToMany(mappedBy = "user", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<EntityNotification> notifications = new ArrayList<>();

    @Lob
    @Column(name = "image_profile", nullable = true)
    private String imageProfile;

    @ManyToMany
    @JsonIgnoreProperties({ "users", "handler", "hibernateLazyInitializer" })
    @JoinTable(name = "users_roles", joinColumns = @JoinColumn(name = "user_id"), inverseJoinColumns = @JoinColumn(name = "role_id"), uniqueConstraints = {
            @UniqueConstraint(columnNames = { "user_id", "role_id" }) })
    private List<RoleEntity> roles;

    public void mapActiveDirectoryUser(UserEntity userAD) {
        this.fullName = userAD.getFullName();
        this.email = userAD.getEmail();
        this.phone = userAD.getPhone();
        this.enabled = userAD.getEnabled();
        this.dateModify = userAD.getDateModify();
        this.cargo = userAD.getCargo();
    }

}
