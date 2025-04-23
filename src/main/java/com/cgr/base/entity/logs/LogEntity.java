package com.cgr.base.entity.logs;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Column;
import lombok.Data;

@Data
@Entity
@Table(name = "logs_login")
public class LogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "date_session_start")
    private String dateSessionStart;

    @Column(name = "user_id")
    private Long userId;

    @Column(name = "roles")
    private String roles;

    @Column(name = "status")
    private String status;

}
