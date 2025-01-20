package com.cgr.base.infrastructure.utilities.dto;

import java.util.List;

import lombok.Data;

@Data
public class PaginationResponse<T> {

    private List<T> content; // List of content (data)
    private int currentPage; // Current page number
    private int totalPages; // Total number of pages
    private long totalElements; // Total number of elements
    private int pageSize; // Size of each page
    private boolean isLastPage; // Whether the current page is the last one

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