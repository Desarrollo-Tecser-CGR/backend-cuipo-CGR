package com.cgr.base.infrastructure.repositories.repositories;

import com.cgr.base.domain.models.entity.ExportCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RepositoryExportCount extends JpaRepository<ExportCount, Long> {

}