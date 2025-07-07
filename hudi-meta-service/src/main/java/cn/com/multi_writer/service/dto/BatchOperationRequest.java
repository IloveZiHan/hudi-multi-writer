package cn.com.multi_writer.service.dto;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 批量操作请求对象
 */
public class BatchOperationRequest {
    
    @NotEmpty(message = "表ID列表不能为空")
    private List<String> ids;
    
    @NotNull(message = "操作类型不能为空")
    private String operation; // "delete", "online", "offline", "export"
    
    private Map<String, Object> params;
    
    // 默认构造函数
    public BatchOperationRequest() {
    }
    
    // 完整构造函数
    public BatchOperationRequest(List<String> ids, String operation, Map<String, Object> params) {
        this.ids = ids;
        this.operation = operation;
        this.params = params;
    }
    
    // Getters and Setters
    public List<String> getIds() {
        return ids;
    }
    
    public void setIds(List<String> ids) {
        this.ids = ids;
    }
    
    public String getOperation() {
        return operation;
    }
    
    public void setOperation(String operation) {
        this.operation = operation;
    }
    
    public Map<String, Object> getParams() {
        return params;
    }
    
    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BatchOperationRequest that = (BatchOperationRequest) o;
        return Objects.equals(ids, that.ids) &&
                Objects.equals(operation, that.operation) &&
                Objects.equals(params, that.params);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(ids, operation, params);
    }
    
    @Override
    public String toString() {
        return "BatchOperationRequest{" +
                "ids=" + ids +
                ", operation='" + operation + '\'' +
                ", params=" + params +
                '}';
    }
} 