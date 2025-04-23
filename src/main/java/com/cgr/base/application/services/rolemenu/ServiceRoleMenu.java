package com.cgr.base.application.services.rolemenu;

import com.cgr.base.domain.dto.dtoRoleMenu.DtoRoleMenu;
import com.cgr.base.domain.models.entity.Logs.RoleEntity;
import com.cgr.base.domain.models.entity.Logs.exit.DinamicRoleSubmenu;
import com.cgr.base.infrastructure.repositories.repositories.repositoryRoleMenu.RepositoryDinamicRoleMenu;
import com.cgr.base.infrastructure.repositories.repositories.role.IRoleRepositoryJpa;
import com.cgr.base.infrastructure.repositories.repositories.subMenu.RepositorySubMenu;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ServiceRoleMenu {

    @Autowired
    private IRoleRepositoryJpa roleRepositoryJpa;

    @Autowired
    private RepositorySubMenu subMenuRepository;

    @Autowired
    private RepositoryDinamicRoleMenu dinamicRoleSubmenuRepository;

    public List<DtoRoleMenu> getRoleMenuByRoleId(Integer roleId) {
        List<DinamicRoleSubmenu> dinamicRoleSubmenus = dinamicRoleSubmenuRepository.findByRoleId(roleId);
        List<DtoRoleMenu> dtos = new ArrayList<>();

        for (DinamicRoleSubmenu relacion : dinamicRoleSubmenus) {
            roleRepositoryJpa.findById(Long.valueOf(relacion.getRole_id()))
                    .ifPresent(role -> subMenuRepository.findById(Long.valueOf(relacion.getSubmenu_id()))
                            .ifPresent(subMenu -> {
                                DtoRoleMenu dto = new DtoRoleMenu();
                                dto.setId(relacion.getId().intValue());
                                dto.setRole_id(role.getId().intValue());
                                dto.setSubmenu_id(subMenu.getId().intValue());
                                dtos.add(dto);
                            }));
        }

        return dtos;
    }

    public List<DtoRoleMenu> getAllRoleMenus() {
        List<DinamicRoleSubmenu> allDinamicRoleSubmenus = dinamicRoleSubmenuRepository.findAll();
        List<DtoRoleMenu> roleMenuDtos = new ArrayList<>();

        for (DinamicRoleSubmenu relacion : allDinamicRoleSubmenus) {
            roleRepositoryJpa.findById(Long.valueOf(relacion.getRole_id()))
                    .ifPresent(role -> subMenuRepository.findById(Long.valueOf(relacion.getSubmenu_id()))
                            .ifPresent(subMenu -> {
                                DtoRoleMenu dto = new DtoRoleMenu();
                                dto.setId(relacion.getId().intValue());
                                dto.setRole_id(role.getId().intValue());
                                dto.setName_rol(role.getName());
                                dto.setSubmenu_id(subMenu.getId().intValue());
                                dto.setSubmenu_title(subMenu.getTitle());
                                dto.setLink(subMenu.getLink());
                                roleMenuDtos.add(dto);
                            }));
        }

        return roleMenuDtos;
    }
}