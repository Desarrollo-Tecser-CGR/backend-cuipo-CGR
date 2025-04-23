package com.cgr.base.repository.menu;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.menu.Menu;

@Repository
public interface IMenuRepositoryJpa extends JpaRepository<Menu,Long> {

    List<Menu> findByTitleNot(String title);

}
