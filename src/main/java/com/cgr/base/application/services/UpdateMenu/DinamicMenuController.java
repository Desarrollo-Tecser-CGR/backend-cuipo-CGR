package com.cgr.base.application.services.UpdateMenu;

import com.cgr.base.application.services.UpdateMenu.DtoDinamicMenu;
import com.cgr.base.application.services.UpdateMenu.ServiceDinamicMenu;
import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/dinamic-menus")
public class DinamicMenuController {

    @Autowired
    private ServiceDinamicMenu serviceDinamicMenu;

    @GetMapping("/dinamicosSubMenu")
    public List<EntityDinamicMenu> getAllDinamicMenus() {
        return serviceDinamicMenu.all();
    }

    @GetMapping("/submenus")
    public List<SubMenuEntity> getAllSubmenus() {
        return serviceDinamicMenu.allSubmenu();
    }

    @PostMapping("/create")
    public ResponseEntity<EntityDinamicMenu> createDinamicMenu(@RequestBody DtoDinamicMenu dtoDinamicMenu) {
        return serviceDinamicMenu.createUserDinamicMenu(dtoDinamicMenu);
    }
}