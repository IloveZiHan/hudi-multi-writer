package cn.com.multi_writer.web.exception;

/**
 * 数据验证异常
 */
public class DataValidationException extends RuntimeException {
    
    private int code;
    
    public DataValidationException(String message) {
        super(message);
        this.code = 400;
    }
    
    public DataValidationException(int code, String message) {
        super(message);
        this.code = code;
    }
    
    public DataValidationException(String message, Throwable cause) {
        super(message, cause);
        this.code = 400;
    }
    
    public DataValidationException(int code, String message, Throwable cause) {
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