package com.cgr.base.infrastructure.repositories.repositories.repositoryNotification;


import com.cgr.base.domain.models.entity.EntityNotification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryNotification extends JpaRepository <EntityNotification, Integer> {

}
