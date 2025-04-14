package com.cgr.base.dto.auth;

import java.util.List;

import com.cgr.base.entity.menu.Menu;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AuthResponseDto {

    private String sAMAccountName;
    private String token;
    private Boolean isEnable;
    private List<RoleDto> roles;
    private List<Menu> menus;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RoleDto {
        private Long id;
        private String name;
    }

}
