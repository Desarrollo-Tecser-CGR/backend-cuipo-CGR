package com.cgr.base.application.access.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

@Service
public class accessManagement {

    @PersistenceContext
    private EntityManager entityManager;

    public List<Map<String, Object>> getAvailableMenus() {
        String sql = "SELECT id, title" +
                " FROM cuipo_dev.dbo.menus " +
                "WHERE title <> 'Gestor de Accesos'";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        return results.stream().map(row -> Map.of(
                "id", row[0],
                "title", row[1])).collect(Collectors.toList());
    }

    public List<Map<String, Object>> getRolesWithMenus() {
        String sql = "SELECT r.id AS role_id, r.name AS role_name, m.id AS menu_id, m.title AS menu_title " +
                "FROM cuipo_dev.dbo.roles r " +
                "LEFT JOIN cuipo_dev.dbo.menu_roles mr ON r.id = mr.role_id " +
                "LEFT JOIN cuipo_dev.dbo.menus m ON mr.menu_id = m.id " +
                "WHERE m.title IS NULL OR m.title <> 'Gestor de Accesos' " +
                "ORDER BY r.id";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        Map<Long, Map<String, Object>> rolesMap = new LinkedHashMap<>();

        for (Object[] row : results) {
            Long roleId = ((Number) row[0]).longValue();
            String roleName = (String) row[1];
            Long menuId = row[2] != null ? ((Number) row[2]).longValue() : null;
            String menuTitle = row[3] != null ? (String) row[3] : null;

            rolesMap.putIfAbsent(roleId, new HashMap<>(Map.of(
                    "role_id", roleId,
                    "role_name", roleName,
                    "modules", new ArrayList<Map<String, Object>>())));

            List<Map<String, Object>> modules = (List<Map<String, Object>>) rolesMap.get(roleId).get("modules");

            if (menuId != null && menuTitle != null) {
                modules.add(Map.of("id", menuId, "title", menuTitle));
            }
        }

        return new ArrayList<>(rolesMap.values());
    }

    @Transactional
    public boolean updateRoleModules(Long roleId, List<Integer> moduleIds) {
        try {
            System.out.println("Iniciando actualización de módulos para el rol ID: " + roleId);
            System.out.println("Lista inicial de módulos: " + moduleIds);

            // Obtener el ID del módulo "Gestor de Accesos"
            Long gestorId = ((Number) entityManager.createNativeQuery(
                    "SELECT id FROM cuipo_dev.dbo.menus WHERE title = 'Gestor de Accesos'")
                    .getSingleResult()).longValue();

            System.out.println("ID de 'Gestor de Accesos': " + gestorId);

            if (roleId == 1) {
                // Si el rol es Administrador (1) y no tiene "Gestor de Accesos", se agrega
                // automáticamente
                if (!moduleIds.contains(gestorId.intValue())) {
                    System.out.println("Rol Administrador sin 'Gestor de Accesos', agregando...");
                    moduleIds.add(gestorId.intValue());
                }
            } else {
                // Para otros roles, si tienen "Gestor de Accesos", se elimina
                if (moduleIds.contains(gestorId.intValue())) {
                    System.out.println("Rol no Administrador con 'Gestor de Accesos', eliminando...");
                    moduleIds.remove(gestorId.intValue());
                }
            }

            System.out.println("Lista de módulos después de validaciones: " + moduleIds);

            // Obtener submódulos de los módulos seleccionados
            String sql = "SELECT id FROM cuipo_dev.dbo.submenus WHERE menu_id IN (:moduleIds)";
            Query query = entityManager.createNativeQuery(sql);
            query.setParameter("moduleIds", moduleIds);

            @SuppressWarnings("unchecked")
            List<Object> result = query.getResultList();

            List<Integer> submoduleIds = result.stream()
                    .map(o -> ((Number) o).intValue())
                    .collect(Collectors.toList());

            System.out.println("Lista de submódulos asociados: " + submoduleIds);

            // Eliminar relaciones previas
            System.out.println("Eliminando relaciones previas en menu_roles y roles_submenu...");
            entityManager.createNativeQuery("DELETE FROM cuipo_dev.dbo.menu_roles WHERE role_id = :roleId")
                    .setParameter("roleId", roleId)
                    .executeUpdate();

            entityManager.createNativeQuery("DELETE FROM cuipo_dev.dbo.roles_submenu WHERE role_id = :roleId")
                    .setParameter("roleId", roleId)
                    .executeUpdate();

            // Insertar nuevas relaciones en menu_roles
            if (!moduleIds.isEmpty()) {
                System.out.println("Insertando nuevas relaciones en menu_roles...");
                for (Integer moduleId : moduleIds) {
                    System.out.println("Asignando módulo: " + moduleId + " al rol: " + roleId);
                    entityManager.createNativeQuery(
                            "INSERT INTO cuipo_dev.dbo.menu_roles (role_id, menu_id) VALUES (:roleId, :moduleId)")
                            .setParameter("roleId", roleId)
                            .setParameter("moduleId", moduleId)
                            .executeUpdate();
                }
            }

            // Insertar nuevas relaciones en roles_submenu
            if (!submoduleIds.isEmpty()) {
                System.out.println("Insertando nuevas relaciones en roles_submenu...");
                for (Integer submoduleId : submoduleIds) {
                    System.out.println("Asignando submódulo: " + submoduleId + " al rol: " + roleId);
                    entityManager.createNativeQuery(
                            "INSERT INTO cuipo_dev.dbo.roles_submenu (role_id, submenu_id) VALUES (:roleId, :submenuId)")
                            .setParameter("roleId", roleId)
                            .setParameter("submenuId", submoduleId)
                            .executeUpdate();
                }
            }

            System.out.println("Actualización de módulos completada exitosamente para el rol ID: " + roleId);
            return true;
        } catch (Exception e) {
            System.err.println("Error al actualizar módulos del rol: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Error al actualizar módulos del rol", e);
        }
    }

}
