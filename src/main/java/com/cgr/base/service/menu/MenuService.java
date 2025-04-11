package com.cgr.base.service.menu;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;

import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;
import com.cgr.base.infrastructure.persistence.repository.menu.IMenuRepositoryJpa;

@Service
public class MenuService {

    private final IMenuRepositoryJpa menuRepositoryJpa;

    public MenuService(IMenuRepositoryJpa menuRepositoryJpa) {
        this.menuRepositoryJpa = menuRepositoryJpa;
    }

    public List<Menu> getAllMenus() {
        return menuRepositoryJpa.findAll();
    }

    // Obtiene los Menus
    public Map<String, Object> getMenus() {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Menu> menus = menuRepositoryJpa.findAll();
            response.put("menus", menus);
            response.put("message", "Menus Retrieved Successfully.");
            response.put("statusCode", 200);
            response.put("status", "success");
            return response;
        } catch (Exception e) {
            response.put("errormsj", e.getMessage());
            response.put("message", "Error Retrieving Menus.");
            response.put("statusCode", 500);
            response.put("status", "error");
            return response;
        }
    }

}
