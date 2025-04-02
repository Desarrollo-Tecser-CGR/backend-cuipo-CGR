package com.cgr.base.application.services.UpdateMenu;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@Entity
@Table (name = "user_submenu")
public class EntityDinamicMenu {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id_dinamic;
    @ManyToOne
    @JoinColumn(name = "id_submenu")
    private SubMenuEntity subMenu;

    @ManyToOne
    @JoinColumn(name = "id_user")
    private UserEntity user;

}
