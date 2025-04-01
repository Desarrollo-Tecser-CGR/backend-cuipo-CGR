package com.cgr.base.domain.dto.dtoLogs.logsExit;

import java.util.Date;

import com.cgr.base.domain.dto.dtoUser.UserDto;

import lombok.Data;

@Data
public class LogExitDto {

    private Date dataSessionEnd;

    private boolean enable;

    private UserDto user;

}
