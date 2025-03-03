package com.cgr.base.infrastructure.repositories.repositories.menu;

import org.springframework.data.jpa.repository.JpaRepository;

import com.cgr.base.domain.models.entity.Menu.Menu;

public interface IMenuRepositoryJpa extends JpaRepository<Menu,Long> {


}
