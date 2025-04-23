package com.cgr.base.common.pagination;

import java.util.List;

import lombok.Data;

@Data
public class PaginationResponse<T> {

    private List<T> content;
    private int currentPage;
    private int totalPages;
    private long totalElements;
    private int pageSize;
    private boolean isLastPage;

    public PaginationResponse(List<T> content, int currentPage, int totalPages, long totalElements, int pageSize,
            boolean isLastPage) {
        this.content = content;
        this.currentPage = currentPage;
        this.totalPages = totalPages;
        this.totalElements = totalElements;
        this.pageSize = pageSize;
        this.isLastPage = isLastPage;
    }
}