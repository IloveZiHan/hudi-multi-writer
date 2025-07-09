package cn.com.multi_writer.service.dto;

import java.util.List;
import java.util.Objects;

/**
 * 批量操作结果对象
 */
public class BatchOperationResult {
    
    private int success;
    
    private int failed;
    
    private int total;
    
    private List<String> errors;
    
    // 默认构造函数
    public BatchOperationResult() {
    }
    
    // 完整构造函数
    public BatchOperationResult(int success, int failed, int total, List<String> errors) {
        this.success = success;
        this.failed = failed;
        this.total = total;
        this.errors = errors;
    }
    
    // Getters and Setters
    public int getSuccess() {
        return success;
    }
    
    public void setSuccess(int success) {
        this.success = success;
    }
    
    public int getFailed() {
        return failed;
    }
    
    public void setFailed(int failed) {
        this.failed = failed;
    }
    
    public int getTotal() {
        return total;
    }
    
    public void setTotal(int total) {
        this.total = total;
    }
    
    public List<String> getErrors() {
        return errors;
    }
    
    public void setErrors(List<String> errors) {
        this.errors = errors;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchOperationResult that = (BatchOperationResult) o;
        return success == that.success &&
                failed == that.failed &&
                total == that.total &&
                Objects.equals(errors, that.errors);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(success, failed, total, errors);
    }
    
    @Override
    public String toString() {
        return "BatchOperationResult{" +
                "success=" + success +
                ", failed=" + failed +
                ", total=" + total +
                ", errors=" + errors +
                '}';
    }
} 