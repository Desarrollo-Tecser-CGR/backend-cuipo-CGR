package com.cgr.base.domain.dto.dtoLogs;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class LoginCountDto {
    private String email;
    private long successfulLogins;
    private long failedLogins;
}
