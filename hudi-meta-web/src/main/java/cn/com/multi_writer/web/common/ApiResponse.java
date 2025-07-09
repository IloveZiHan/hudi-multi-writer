package cn.com.multi_writer.web.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 统一API响应格式
 * @param <T> 响应数据类型
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {
    
    /**
     * 响应状态码
     */
    @JsonProperty("code")
    private int code;
    
    /**
     * 响应消息
     */
    @JsonProperty("message")
    private String message;
    
    /**
     * 响应数据
     */
    @JsonProperty("data")
    private T data;
    
    /**
     * 时间戳
     */
    @JsonProperty("timestamp")
    private String timestamp;
    
    // 常用状态码常量
    public static final int SUCCESS_CODE = 200;
    public static final int CREATED_CODE = 201;
    public static final int BAD_REQUEST_CODE = 400;
    public static final int NOT_FOUND_CODE = 404;
    public static final int CONFLICT_CODE = 409;
    public static final int INTERNAL_ERROR_CODE = 500;
    
    /**
     * 默认构造函数
     */
    public ApiResponse() {
        this.timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }
    
    /**
     * 完整构造函数
     * @param code 状态码
     * @param message 消息
     * @param data 数据
     */
    public ApiResponse(int code, String message, T data) {
        this();
        this.code = code;
        this.message = message;
        this.data = data;
    }
    
    /**
     * 构造函数（不包含数据）
     * @param code 状态码
     * @param message 消息
     */
    public ApiResponse(int code, String message) {
        this(code, message, null);
    }
    
    // 静态工厂方法
    
    /**
     * 成功响应
     * @param data 响应数据
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(SUCCESS_CODE, "操作成功", data);
    }
    
    /**
     * 成功响应（无数据）
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> success() {
        return new ApiResponse<>(SUCCESS_CODE, "操作成功");
    }
    
    /**
     * 成功响应（自定义消息）
     * @param message 自定义消息
     * @param data 响应数据
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> success(String message, T data) {
        return new ApiResponse<>(SUCCESS_CODE, message, data);
    }
    
    /**
     * 创建成功响应
     * @param message 消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> created(String message) {
        return new ApiResponse<>(CREATED_CODE, message);
    }
    
    /**
     * 创建成功响应（包含数据）
     * @param message 消息
     * @param data 响应数据
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> created(String message, T data) {
        return new ApiResponse<>(CREATED_CODE, message, data);
    }
    
    /**
     * 错误响应
     * @param code 错误码
     * @param message 错误消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> error(int code, String message) {
        return new ApiResponse<>(code, message);
    }
    
    /**
     * 错误响应（包含数据）
     * @param code 错误码
     * @param message 错误消息
     * @param data 响应数据
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> error(int code, String message, T data) {
        return new ApiResponse<>(code, message, data);
    }
    
    /**
     * 400 错误请求
     * @param message 错误消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> badRequest(String message) {
        return new ApiResponse<>(BAD_REQUEST_CODE, message);
    }
    
    /**
     * 400 错误请求（包含数据）
     * @param message 错误消息
     * @param data 响应数据
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> badRequest(String message, T data) {
        return new ApiResponse<>(BAD_REQUEST_CODE, message, data);
    }
    
    /**
     * 404 资源未找到
     * @param message 错误消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> notFound(String message) {
        return new ApiResponse<>(NOT_FOUND_CODE, message);
    }
    
    /**
     * 409 资源冲突
     * @param message 错误消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> conflict(String message) {
        return new ApiResponse<>(CONFLICT_CODE, message);
    }
    
    /**
     * 500 内部服务器错误
     * @param message 错误消息
     * @param <T> 数据类型
     * @return UnifiedResponse实例
     */
    public static <T> ApiResponse<T> internalError(String message) {
        return new ApiResponse<>(INTERNAL_ERROR_CODE, message);
    }
    
    // Getters and Setters
    
    public int getCode() {
        return code;
    }
    
    public void setCode(int code) {
        this.code = code;
    }
    
    public String getMessage() {
        return message;
    }
    
    public void setMessage(String message) {
        this.message = message;
    }
    
    public T getData() {
        return data;
    }
    
    public void setData(T data) {
        this.data = data;
    }
    
    public String getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }
    
    /**
     * 判断是否成功
     * @return 是否成功
     */
    public boolean isSuccess() {
        return this.code >= 200 && this.code < 300;
    }
    
    @Override
    public String toString() {
        return "UnifiedResponse{" +
                "code=" + code +
                ", message='" + message + '\'' +
                ", data=" + data +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
} 