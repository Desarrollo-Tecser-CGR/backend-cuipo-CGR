package com.cgr.base.application.services.role.service.permission;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public enum Permission {

    ADMIN_UPDATE ("admin:update"),
    ADMIN_CREATE ("admin:create"),
    ADMIN_INACTIVE("admin:Inactive"),

    AUDITOR_LETTER ("auditor:letter"),
    AUDITOR_EXPORT ("AUDITOR:EXPORT"),

    ANALISTA_LETTER ("ANALISTA_LETTER");





    @Getter
    private final String permission;
}
