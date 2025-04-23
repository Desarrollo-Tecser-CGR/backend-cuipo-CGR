package com.cgr.base.repository.access;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.access.MenuRole;
import com.cgr.base.entity.access.MenuRole.MenuRoleId;

@Repository
public interface MenuRoleRepo extends JpaRepository<MenuRole, MenuRoleId> {

    List<MenuRole> findByRoleId(Long roleId);

}
