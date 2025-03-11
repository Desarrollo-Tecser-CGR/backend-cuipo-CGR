package com.cgr.base.application.notificaction.repository;


import com.cgr.base.application.notificaction.entity.EntityNotification;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface RepositoryNotification extends JpaRepository <EntityNotification, Integer> {

}
