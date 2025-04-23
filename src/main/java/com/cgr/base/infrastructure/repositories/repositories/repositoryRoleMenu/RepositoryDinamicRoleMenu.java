package com.cgr.base.infrastructure.repositories.repositories.repositoryRoleMenu;

import com.cgr.base.domain.models.entity.Logs.exit.DinamicRoleSubmenu;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface RepositoryDinamicRoleMenu extends JpaRepository<DinamicRoleSubmenu, Integer> {

    List<DinamicRoleSubmenu> findAll();

    @Query("SELECT d FROM DinamicRoleSubmenu d WHERE d.role_id = :roleId")
    List<DinamicRoleSubmenu> findByRoleId(Integer roleId);
}