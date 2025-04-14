package com.cgr.base.repository.parametrization;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.stereotype.Repository;

import com.cgr.base.entity.parametrization.ParametrizacionAnual;

import java.util.List;
import java.util.Optional;

@Repository
public class ParametrizacionAnualRepository {

    @PersistenceContext
    private EntityManager entityManager;

    public Optional<ParametrizacionAnual> findByFecha(int fecha) {
        return Optional.ofNullable(entityManager.find(ParametrizacionAnual.class, fecha));
    }

    public ParametrizacionAnual save(ParametrizacionAnual parametrizacionAnual) {
        entityManager.persist(parametrizacionAnual);
        return parametrizacionAnual;
    }

    public ParametrizacionAnual update(ParametrizacionAnual parametrizacionAnual) {
        return entityManager.merge(parametrizacionAnual);
    }

    public void deleteByFecha(int fecha) {
        ParametrizacionAnual entity = entityManager.find(ParametrizacionAnual.class, fecha);
        if (entity != null) {
            entityManager.remove(entity);
        }
    }

    public List<ParametrizacionAnual> findAll() {
        return entityManager.createQuery("SELECT p FROM ParametrizacionAnual p", ParametrizacionAnual.class)
                .getResultList();
    }
}
