package com.cgr.base.infrastructure.persistence.repository.menu;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.Menu.Menu;

@Repository
public interface IMenuRepositoryJpa extends JpaRepository<Menu,Long> {

    List<Menu> findByTitleNot(String title);

}
