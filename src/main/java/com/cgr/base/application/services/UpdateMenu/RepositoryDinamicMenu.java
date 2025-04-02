package com.cgr.base.application.services.UpdateMenu;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryDinamicMenu extends JpaRepository<EntityDinamicMenu,Long> {

}
