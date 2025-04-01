package com.cgr.base.application.logs.service;

import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.stereotype.Service;

import com.cgr.base.application.logs.dto.LogWithUserFullNameDTO;
import com.cgr.base.infrastructure.persistence.entity.log.LogType;
import com.cgr.base.infrastructure.persistence.entity.log.LogsEntityGeneral;
import com.cgr.base.infrastructure.persistence.repository.logs.ILogGeneralRepositoryJpa;

@Service
public class LogGeneralService {

    private final ILogGeneralRepositoryJpa logRepository;

    public LogGeneralService(ILogGeneralRepositoryJpa logRepository) {
        this.logRepository = logRepository;
    }

    public LogsEntityGeneral createLog(Long user, LogType logType, String detail) {
        LogsEntityGeneral log = new LogsEntityGeneral(user, logType, detail);
        return logRepository.save(log);
    }

    public List<LogWithUserFullNameDTO> findLogsByFilters(Long userId, LogType logType, String detail,
            String createdAt) {
        List<Object[]> results = logRepository.findLogsWithUserFullNameByFiltersNative(
                userId,
                logType != null ? logType.name() : null,
                detail,
                createdAt);

        return results.stream()
                .map(obj -> {
                    Long id = ((Number) obj[0]).longValue();
                    Long userIdVal = ((Number) obj[1]).longValue();
                    LogType type = LogType.valueOf((String) obj[2]);
                    String detailVal = (String) obj[3];
                    String createdAtVal = obj[4] instanceof Timestamp
                            ? ((Timestamp) obj[4]).toLocalDateTime()
                                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                            : obj[4].toString(); // fallback por si viene como otro tipo

                    String fullName = (String) obj[obj.length - 1];

                    return new LogWithUserFullNameDTO(id, userIdVal, type, detailVal, createdAtVal, fullName);
                })
                .collect(Collectors.toList());
    }

}
