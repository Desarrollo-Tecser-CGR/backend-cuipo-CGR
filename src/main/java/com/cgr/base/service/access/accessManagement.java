package com.cgr.base.service.access;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.persistence.Query;

@Service
public class accessManagement {

    @PersistenceContext
    private EntityManager entityManager;

    @Value("${DATASOURCE_NAME}")
    private String DATASOURCE_NAME;

    public List<Map<String, Object>> getAvailableMenus() {
        String sql = "SELECT id, title" +
                " FROM " + DATASOURCE_NAME + ".dbo.menus " +
                " WHERE id <> 1";

        Query query = entityManager.createNativeQuery(sql);

        @SuppressWarnings("unchecked")
        List<Object[]> results = (List<Object[]>) query.getResultList();

        return results.stream().map(row -> Map.of(
                "id", row[0],
                "title", row[1])).collect(Collectors.toList());
    }

    public List<Map<String, Object>> getRolesWithMenus() {
        String sql = "SELECT r.id AS role_id, r.name AS role_name, m.id AS menu_id, m.title AS menu_title, m.description "
                +
                "FROM " + DATASOURCE_NAME + ".dbo.roles r " +
                "LEFT JOIN " + DATASOURCE_NAME + ".dbo.menu_roles mr ON r.id = mr.role_id " +
                "LEFT JOIN " + DATASOURCE_NAME + ".dbo.menus m ON mr.menu_id = m.id " +
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
            String description = (String) row[4];

            rolesMap.putIfAbsent(roleId, new HashMap<>(Map.of(
                    "role_id", roleId,
                    "role_name", roleName,
                    "modules", new ArrayList<Map<String, Object>>())));

            List<Map<String, Object>> modules = (List<Map<String, Object>>) rolesMap.get(roleId).get("modules");

            if (menuId != null && menuTitle != null) {
                modules.add(Map.of("id", menuId, "title", menuTitle, "description", description));
            }
        }

        return new ArrayList<>(rolesMap.values());
    }

    @Transactional
    public boolean updateRoleModules(Long roleId, List<Integer> moduleIds) {
        try {

            int gestorId = 1;

            if (roleId == 1) {

                if (!moduleIds.contains(gestorId)) {
                    moduleIds.add(gestorId);
                }
            } else {
                if (moduleIds.contains(gestorId)) {
                    moduleIds.remove(gestorId);
                }
            }

            entityManager
                    .createNativeQuery("DELETE FROM " + DATASOURCE_NAME + ".dbo.menu_roles WHERE role_id = :roleId")
                    .setParameter("roleId", roleId)
                    .executeUpdate();

            if (!moduleIds.isEmpty()) {
                for (Integer moduleId : moduleIds) {
                    entityManager.createNativeQuery(
                            "INSERT INTO " + DATASOURCE_NAME
                                    + ".dbo.menu_roles (role_id, menu_id) VALUES (:roleId, :moduleId)")
                            .setParameter("roleId", roleId)
                            .setParameter("moduleId", moduleId)
                            .executeUpdate();
                }
            }

            return true;
        } catch (Exception e) {

            throw new RuntimeException("Error al actualizar módulos del rol", e);
        }
    }

    public boolean roleExists(Long roleId) {
        String sql = "SELECT COUNT(*) FROM " + DATASOURCE_NAME + ".dbo.roles WHERE id = :roleId";
        Query query = entityManager.createNativeQuery(sql).setParameter("roleId", roleId);
        return ((Number) query.getSingleResult()).intValue() > 0;
    }

    public List<Integer> getInvalidModules(List<Integer> moduleIds) {
        String sql = "SELECT id FROM " + DATASOURCE_NAME + ".dbo.menus WHERE id IN :moduleIds";
        Query query = entityManager.createNativeQuery(sql).setParameter("moduleIds", moduleIds);

        @SuppressWarnings("unchecked")
        List<Number> validModuleIds = query.getResultList();

        return moduleIds.stream()
                .filter(moduleId -> validModuleIds.stream().noneMatch(validId -> validId.intValue() == moduleId))
                .collect(Collectors.toList());
    }

    @Transactional
    public void assignMenuCommentsPermissionToRole(Long roleId) {
        String sqlCheckPermission = """
                    SELECT COUNT(*) FROM menu_roles
                    WHERE role_id = :roleId AND menu_id = (
                        SELECT id FROM menus WHERE code = 'MENU_COMMENTS'
                    )
                """;

        int count = ((Number) entityManager.createNativeQuery(sqlCheckPermission)
                .setParameter("roleId", roleId)
                .getSingleResult()).intValue();

        if (count == 0) {
            String sqlAssignPermission = """
                        INSERT INTO menu_roles (role_id, menu_id)
                        SELECT :roleId, id FROM menus WHERE code = 'MENU_COMMENTS'
                    """;
            entityManager.createNativeQuery(sqlAssignPermission)
                    .setParameter("roleId", roleId)
                    .executeUpdate();
        }
    }

}
