package com.cgr.base.infrastructure.persistence.entity.user;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "user_profile")
@Data
@NoArgsConstructor
public class ProfileEntity {

    @Id
    @Column(name = "user_id")
    private Long userId;

    @Column(name = "image_profile", columnDefinition = "NVARCHAR(MAX)", nullable = true)
    private String imageProfile;
    
}
