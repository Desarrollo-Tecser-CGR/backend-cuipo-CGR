package com.cgr.base.application.services.UpdateMenu;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import lombok.Data;

@Data
public class DtoDinamicMenu {

    private Long id_dinamic;
    private SubMenuEntity subMenu;
    private UserEntity user;
}
