package com.cgr.base.infrastructure.persistence.specification.filter;

import java.util.Date;

import org.springframework.data.jpa.domain.Specification;

import com.cgr.base.application.user.dto.UserFilterRequestDto;
import com.cgr.base.infrastructure.persistence.entity.user.UserEntity;

import jakarta.persistence.criteria.Predicate;

public class UserSpecification {
    public static Specification<UserEntity> conFiltros(UserFilterRequestDto filtro) {
        return (root, query, criteriaBuilder) -> {
            Predicate predicate = criteriaBuilder.conjunction();

            // Filtrar por sAMAccountName
            if (filtro.getUserName() != null && !filtro.getUserName().equals("")) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.like(criteriaBuilder.lower(root.get("sAMAccountName")),
                                "%" + filtro.getUserName().toLowerCase() + "%"));
            }

            // Filtrar por fullName
            if (filtro.getFullName() != null && !filtro.getFullName().equals("")) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.like(criteriaBuilder.lower(root.get("fullName")),
                                "%" + filtro.getFullName().toLowerCase() + "%"));
            }

            // Filtrar por email
            if (filtro.getEmail() != null && !filtro.getEmail().equals("")) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.like(criteriaBuilder.lower(root.get("email")),
                                "%" + filtro.getEmail().toLowerCase() + "%"));
            }

            // Filtrar por phone
            if (filtro.getPhone() != null && !filtro.getPhone().equals("")) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.like(criteriaBuilder.lower(root.get("phone")),
                                "%" + filtro.getPhone().toLowerCase() + "%"));
            }

            // Filtrar por enabled (Boolean)
            if (filtro.getEnabled() != null) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.equal(root.get("enabled"), filtro.getEnabled()));
            }

            // Filtrar por fecha de modificación (dateModify) dentro del rango de startDate
            // y endDate
            if (filtro.getStartDate() != null && filtro.getEndDate() != null) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.between(root.get("dateModify").as(Date.class), filtro.getStartDate(),
                                filtro.getEndDate()));
            } else if (filtro.getStartDate() != null) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.greaterThanOrEqualTo(root.get("dateModify").as(Date.class),
                                filtro.getStartDate()));
            } else if (filtro.getEndDate() != null) {
                predicate = criteriaBuilder.and(predicate,
                        criteriaBuilder.lessThanOrEqualTo(root.get("dateModify").as(Date.class), filtro.getEndDate()));
            }

            return predicate;
        };
    }

}
