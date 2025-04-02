package com.cgr.base.application.services.UpdateMenu;

import com.cgr.base.domain.models.entity.Logs.UserEntity;
import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import com.cgr.base.infrastructure.repositories.repositories.subMenu.RepositorySubMenu;
import com.cgr.base.infrastructure.repositories.repositories.user.IUserRepositoryJpa;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ServiceDinamicMenu {

    @Autowired
    private RepositoryDinamicMenu repositoryDinamicMenu;

    @Autowired
    private RepositorySubMenu repositorySubMenu;

    @Autowired
    private IUserRepositoryJpa repositoryUser; // Inyecta el repositorio de usuarios
 // lista de lo que hay en el menu dinamico
    public List<EntityDinamicMenu> all() {
        return repositoryDinamicMenu.findAll();
    }
// lista de los submenu
    public List<SubMenuEntity> allSubmenu() {
        return repositorySubMenu.findAll();
    }

// crear el permiso para el usuario dinamico
    public ResponseEntity<EntityDinamicMenu> createUserDinamicMenu(DtoDinamicMenu dtoDinamicMenu) {
        try {
            // 1. Obtener las entidades SubMenuEntity y UserEntity usando los IDs del DTO
            SubMenuEntity subMenu = repositorySubMenu.findById(dtoDinamicMenu.getId_dinamic()).orElse(null);
            UserEntity user = repositoryUser.findById(dtoDinamicMenu.getId_dinamic()).orElse(null);

            // 2. Verificar si las entidades existen
            if (subMenu == null || user == null) {
                return new ResponseEntity<>(HttpStatus.BAD_REQUEST); // Retorna un error si no se encuentran las entidades
            }


            EntityDinamicMenu entityDinamicMenu = new EntityDinamicMenu();
            entityDinamicMenu.setId_dinamic(dtoDinamicMenu.getId_dinamic());
            entityDinamicMenu.setSubMenu(subMenu);
            entityDinamicMenu.setUser(user);


            EntityDinamicMenu savedMenu = repositoryDinamicMenu.save(entityDinamicMenu);

            return new ResponseEntity<>(savedMenu, HttpStatus.CREATED);
        } catch (Exception e) {
            return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}