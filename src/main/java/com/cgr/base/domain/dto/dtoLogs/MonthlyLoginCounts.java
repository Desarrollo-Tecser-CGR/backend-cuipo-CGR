package com.cgr.base.domain.dto.dtoLogs;

public class MonthlyLoginCounts {
    private int year;
    private int month;
    private int success;
    private int failure;

    public MonthlyLoginCounts(int year, int month, int success, int failure) {
        this.year = year;
        this.month = month;
        this.success = success;
        this.failure = failure;
    }

    // Getters
    public int getYear() {
        return year;
    }

    public int getMonth() {
        return month;
    }

    public int getSuccess() {
        return success;
    }

    public int getFailure() {
        return failure;
    }
}