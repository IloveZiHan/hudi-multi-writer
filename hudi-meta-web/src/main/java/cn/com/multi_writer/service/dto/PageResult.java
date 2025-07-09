package cn.com.multi_writer.service.dto;

import java.util.List;
import java.util.Objects;

/**
 * 分页结果对象
 */
public class PageResult<T> {
    
    private List<T> data;
    
    private long total;
    
    private int page;
    
    private int size;
    
    private int totalPages;
    
    private boolean hasNext;
    
    private boolean hasPrevious;
    
    // 默认构造函数
    public PageResult() {
    }
    
    // 完整构造函数
    public PageResult(List<T> data, long total, int page, int size) {
        this.data = data;
        this.total = total;
        this.page = page;
        this.size = size;
        this.totalPages = (int) Math.ceil((double) total / size);
        this.hasNext = page < totalPages - 1;
        this.hasPrevious = page > 0;
    }
    
    // Getters and Setters
    public List<T> getData() {
        return data;
    }
    
    public void setData(List<T> data) {
        this.data = data;
    }
    
    public long getTotal() {
        return total;
    }
    
    public void setTotal(long total) {
        this.total = total;
    }
    
    public int getPage() {
        return page;
    }
    
    public void setPage(int page) {
        this.page = page;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    public int getTotalPages() {
        return totalPages;
    }
    
    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }
    
    public boolean isHasNext() {
        return hasNext;
    }
    
    public void setHasNext(boolean hasNext) {
        this.hasNext = hasNext;
    }
    
    public boolean isHasPrevious() {
        return hasPrevious;
    }
    
    public void setHasPrevious(boolean hasPrevious) {
        this.hasPrevious = hasPrevious;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PageResult<?> that = (PageResult<?>) o;
        return total == that.total &&
                page == that.page &&
                size == that.size &&
                totalPages == that.totalPages &&
                hasNext == that.hasNext &&
                hasPrevious == that.hasPrevious &&
                Objects.equals(data, that.data);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(data, total, page, size, totalPages, hasNext, hasPrevious);
    }
    
    @Override
    public String toString() {
        return "PageResult{" +
                "data=" + data +
                ", total=" + total +
                ", page=" + page +
                ", size=" + size +
                ", totalPages=" + totalPages +
                ", hasNext=" + hasNext +
                ", hasPrevious=" + hasPrevious +
                '}';
    }
} 