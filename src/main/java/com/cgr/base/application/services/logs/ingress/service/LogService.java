package com.cgr.base.application.services.logs.ingress.service;

import com.cgr.base.domain.dto.dtoLogs.MonthlyLoginCounts; // Import del DTO correcto
import com.cgr.base.domain.models.entity.Logs.LogEntity;
import com.cgr.base.infrastructure.repositories.repositories.repositoryActiveDirectory.ILogRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class LogService {

    @Autowired
    private ILogRepository adapterLogRepository;

    public MonthlyLoginCounts countSuccessfulAndFailedLogins() {
        List<LogEntity> logs = adapterLogRepository.logFindAll();

        int successCount = 0;
        int failureCount = 0;

        for (LogEntity log : logs) {
            if (log.getTipe_of_income() != null && log.getTipe_of_income().trim().equalsIgnoreCase("Éxito")) {
                successCount++;
            } else if (log.getTipe_of_income() != null) {
                failureCount++;
            }
        }

        return new MonthlyLoginCounts(0, 0, successCount, failureCount); // Usando MonthlyLoginCounts
    }

    public List<MonthlyLoginCounts> countSuccessfulAndFailedLoginsByMonth(int year) {
        List<LogEntity> logs = adapterLogRepository.logFindAll();
        List<MonthlyLoginCounts> monthlyCounts = new ArrayList<>();

        for (int month = 1; month <= 12; month++) {
            final int currentMonth = month; // Needed for lambda

            List<LogEntity> filteredLogs = logs.stream()
                    .filter(log -> log.getData_session_start() != null)
                    .filter(log -> {
                        Date date = log.getData_session_start();
                        LocalDateTime dateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                        return dateTime.getYear() == year && dateTime.getMonthValue() == currentMonth;
                    })
                    .collect(Collectors.toList());

            int successCount = 0;
            int failureCount = 0;

            for (LogEntity log : filteredLogs) {
                if (log.getTipe_of_income() != null && log.getTipe_of_income().trim().equalsIgnoreCase("Éxito")) {
                    successCount++;
                } else if (log.getTipe_of_income() != null) {
                    failureCount++;
                }
            }

            monthlyCounts.add(new MonthlyLoginCounts(year, month, successCount, failureCount));
        }

        return monthlyCounts;
    }

    public MonthlyLoginCounts countSuccessfulAndFailedLoginsByYear(int year) {
        List<LogEntity> logs = adapterLogRepository.logFindAll();

        List<LogEntity> filteredLogs = logs.stream()
                .filter(log -> log.getData_session_start() != null)
                .filter(log -> {
                    Date date = log.getData_session_start();
                    LocalDateTime dateTime = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                    return dateTime.getYear() == year;
                })
                .collect(Collectors.toList());

        int successCount = 0;
        int failureCount = 0;

        for (LogEntity log : filteredLogs) {
            if (log.getTipe_of_income() != null && log.getTipe_of_income().trim().equalsIgnoreCase("Éxito")) {
                successCount++;
            } else if (log.getTipe_of_income() != null) {
                failureCount++;
            }
        }

        return new MonthlyLoginCounts(year, 0, successCount, failureCount); // Usando MonthlyLoginCounts
    }
}