package com.cgr.base.infrastructure.persistence.repository.access;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.access.MenuRole;
import com.cgr.base.infrastructure.persistence.entity.access.MenuRole.MenuRoleId;

@Repository
public interface MenuRoleRepo extends JpaRepository<MenuRole, MenuRoleId> {

    List<MenuRole> findByRoleId(Long roleId);

}
