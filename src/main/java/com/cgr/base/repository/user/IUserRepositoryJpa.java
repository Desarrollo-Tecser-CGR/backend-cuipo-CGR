
package com.cgr.base.repository.user;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.menu.Menu;
import com.cgr.base.entity.menu.SubMenuEntity;
import com.cgr.base.entity.user.UserEntity;

@Repository
public interface IUserRepositoryJpa extends JpaRepository<UserEntity, Long>, JpaSpecificationExecutor<UserEntity> {

        @Query("SELECT u FROM UserEntity u WHERE u.sAMAccountName = :sAMAccountName")
        Optional<UserEntity> findBySAMAccountName(@Param("sAMAccountName") String sAMAccountName);

        @Query("SELECT CASE WHEN COUNT(u) > 0 THEN true ELSE false END FROM UserEntity u WHERE u.sAMAccountName = :sAMAccountName")
        boolean existsBySAMAccountName(@Param("sAMAccountName") String sAMAccountName);

        @Query("SELECT u FROM UserEntity u LEFT JOIN FETCH u.roles WHERE u.sAMAccountName = :sAMAccountName")
        Optional<UserEntity> findBySAMAccountNameWithRoles(@Param("sAMAccountName") String sAMAccountName);

        @Query("""
                            SELECT DISTINCT m
                            FROM Menu m
                                 LEFT JOIN FETCH m.roles mr
                                 LEFT JOIN FETCH m.children c
                                 LEFT JOIN FETCH c.roles cr
                            WHERE mr.name IN :roleNames
                               OR cr.name IN :roleNames
                        """)
        List<Menu> findMenusByRoleNames(@Param("roleNames") List<String> roleNames);

        @Query("SELECT DISTINCT sm FROM RoleEntity r " +
                        "JOIN r.subMenus sm " +
                        "WHERE r.name IN :roleNames")
        List<SubMenuEntity> findDistinctSubMenusByRoleNames(@Param("roleNames") List<String> roleNames);

        @Query("SELECT u.fullName FROM UserEntity u WHERE u.id = :id")
        String findFullNameById(@Param("id") Long id);
}
