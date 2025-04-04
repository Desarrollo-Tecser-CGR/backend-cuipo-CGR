package com.cgr.base.infrastructure.repositories.repositories;

import com.cgr.base.domain.models.entity.ExportCount;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface RepositoryExportCount extends JpaRepository<ExportCount, Long> {

    @Query("SELECT DISTINCT YEAR(e.exportDate) AS year, MONTH(e.exportDate) AS month FROM ExportCount e ORDER BY YEAR(e.exportDate), MONTH(e.exportDate)")
    List<Object[]> findDistinctMonthsAndYears();


    @Query("SELECT YEAR(e.exportDate) AS year, MONTH(e.exportDate) AS month, COUNT(e) " +
            "FROM ExportCount e " +
            "GROUP BY YEAR(e.exportDate), MONTH(e.exportDate) " +
            "ORDER BY YEAR(e.exportDate), MONTH(e.exportDate)")
    List<Object[]> countExportsByMonthAndYear();

    @Query("SELECT COUNT(e) FROM ExportCount e")
    long count();


}