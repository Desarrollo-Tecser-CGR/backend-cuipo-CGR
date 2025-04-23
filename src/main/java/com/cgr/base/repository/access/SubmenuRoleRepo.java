package com.cgr.base.repository.access;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.access.SubmenuRole;
import com.cgr.base.entity.access.SubmenuRole.SubmenuRoleId;

@Repository
public interface SubmenuRoleRepo extends JpaRepository<SubmenuRole, SubmenuRoleId> {

    List<SubmenuRole> findByRoleId(Long roleId);

}
