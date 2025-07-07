package cn.com.multi_writer.web.exception;

/**
 * 资源冲突异常
 */
public class ResourceConflictException extends RuntimeException {
    
    private int code;
    
    public ResourceConflictException(String message) {
        super(message);
        this.code = 409;
    }
    
    public ResourceConflictException(int code, String message) {
        super(message);
        this.code = code;
    }
    
    public ResourceConflictException(String message, Throwable cause) {
        super(message, cause);
        this.code = 409;
    }
    
    public ResourceConflictException(int code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }
    
    public int getCode() {
        return code;
    }
    
    public void setCode(int code) {
        this.code = code;
    }
} 