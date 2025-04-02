package com.cgr.base.infrastructure.repositories.repositories.subMenu;

import com.cgr.base.domain.models.entity.Menu.SubMenuEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositorySubMenu extends JpaRepository<SubMenuEntity, Long> {
}
