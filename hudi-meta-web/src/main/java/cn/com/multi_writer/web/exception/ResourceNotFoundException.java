package cn.com.multi_writer.web.exception;

/**
 * 资源未找到异常
 */
public class ResourceNotFoundException extends RuntimeException {
    
    private int code;
    
    public ResourceNotFoundException(String message) {
        super(message);
        this.code = 404;
    }
    
    public ResourceNotFoundException(int code, String message) {
        super(message);
        this.code = code;
    }
    
    public ResourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
        this.code = 404;
    }
    
    public ResourceNotFoundException(int code, String message, Throwable cause) {
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