package com.cgr.base.infrastructure.persistence.repository.logs;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;

@Repository
public interface ILogGeneralRepositoryJpa extends JpaRepository<LogsEntityGeneral, Long> {
}
