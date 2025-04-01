package com.cgr.base.domain.dto.dtoWebSocket;

import java.util.Date;

import com.cgr.base.domain.dto.dtoUser.UserDto;
import com.cgr.base.domain.models.entity.EntityProvitionalPlan;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class EntityNotificationDto {

    private Integer id;

    private String numbercontract;

    private String subject;

    private String notification;

    private Date date;

    private EntityProvitionalPlan entity;

    private UserDto user;

}
