package com.cgr.base.infrastructure.security.endpoints;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class endpointsSecurity {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public Map<String, Set<String>> getEndpointsWithRoles() {
        String sql = """
                    SELECT e.url, r.name
                    FROM endpoints e
                    LEFT JOIN menus_endpoints me ON e.id = me.endpoint_id
                    LEFT JOIN menu_roles mr ON me.menu_id = mr.menu_id
                    LEFT JOIN roles r ON mr.role_id = r.id
                    WHERE e.type = 'RESTRINGIDO'
                """;

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

        Map<String, Set<String>> endpointRolesMap = new HashMap<>();

        for (Map<String, Object> row : rows) {
            String url = (String) row.get("url");
            String role = (String) row.get("name");

            endpointRolesMap
                    .computeIfAbsent(url, k -> new HashSet<>())
                    .add(role);
        }

        return endpointRolesMap;
    }

}
