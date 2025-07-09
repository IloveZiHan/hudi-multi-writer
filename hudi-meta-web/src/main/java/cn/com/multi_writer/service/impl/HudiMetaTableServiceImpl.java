package cn.com.multi_writer.service.impl;

import cn.com.multi_writer.meta.MetaHudiTableManager;
import cn.com.multi_writer.service.MetaTableService;
import cn.com.multi_writer.service.dto.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Meta Hudi Table 服务实现类
 * 基于现有的MetaHudiTableManager实现业务逻辑
 */
@Service
public class HudiMetaTableServiceImpl implements MetaTableService {
    
    private static final Logger logger = LoggerFactory.getLogger(HudiMetaTableServiceImpl.class);
    
    @Value("${hudi.meta.table.path}")
    private String metaTablePath;
    
    private SparkSession sparkSession;
    private MetaHudiTableManager metaManager;
    private ObjectMapper objectMapper;
    
    // 支持的数据库类型
    private static final List<String> SUPPORTED_DB_TYPES = Arrays.asList(
            "MySQL", "PostgreSQL", "Oracle", "SQL Server", "TDSQL", "ClickHouse", "TiDB"
    );
    
    @PostConstruct
    public void init() {
        // 初始化SparkSession（在实际应用中应该通过配置注入）
        this.sparkSession = SparkSession.builder()
                .appName("MetaTableService")
                .master("local[*]")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
                .config("spark.ui.enabled", "true")
                .config("spark.ui.port", "4040")
                .config("spark.hive.metastore.uris", "thrift://huoshan-test04:9083,thrift://huoshan-test03:9083,thrift://huoshan-test05:9083")
                .config("hoodie.table.metadata.enable", "false")
                .config("hoodie.metadata.enable", "false")
                .config("spark.sql.strict", "false")
                .config("spark.sql.streaming.kafka.useDeprecatedOffsetFetching", "false")
                .getOrCreate();
                
        this.metaManager = new MetaHudiTableManager(sparkSession);
        this.objectMapper = new ObjectMapper();
        
        logger.info("MetaTableService初始化完成，metaTablePath: {}", metaTablePath);
    }
    
    @Override
    public PageResult<MetaTableDTO> getAllTables(int page, int size) {
        try {
            logger.info("获取所有表，页码: {}, 每页大小: {}", page, size);
            
            // 读取所有数据
            Dataset<Row> allTablesDF = sparkSession.read().format("hudi").load(metaTablePath);
            long total = allTablesDF.count();
            
            // 分页处理
            long offset = (long) page * size;
            Dataset<Row> pagedDF = allTablesDF
                    .where("cdc_delete_flag = 0")
                    .orderBy(allTablesDF.col("update_time").desc())
                    .limit(size)
                    .offset((int) offset);
            
            List<MetaTableDTO> tables = convertDatasetToMetaTableDTO(pagedDF);
            
            return new PageResult<>(tables, total, page, size);
            
        } catch (Exception e) {
            logger.error("获取所有表失败", e);
            throw new RuntimeException("获取所有表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public Optional<MetaTableDTO> getTableById(String id) {
        try {
            logger.info("根据ID获取表详情: {}", id);
            
            // 根据id过滤
            Dataset<Row> tableDF = sparkSession.read().format("hudi").load(metaTablePath)
                    .filter("id = '" + id + "'");
            
            if (tableDF.count() == 0) {
                return Optional.empty();
            }
            
            List<MetaTableDTO> tables = convertDatasetToMetaTableDTO(tableDF);
            return tables.isEmpty() ? Optional.empty() : Optional.of(tables.get(0));
            
        } catch (Exception e) {
            logger.error("根据ID获取表详情失败: {}", id, e);
            throw new RuntimeException("根据ID获取表详情失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean createTable(CreateTableRequest request) {
        try {
            logger.info("创建新表: {}", request.getId());
            
            // 检查表ID是否已存在
            if (existsTableId(request.getId())) {
                throw new IllegalArgumentException("表ID " + request.getId() + " 已存在");
            }
            
            // 验证schema
            validateTableSchema(request.getSchema());
            
            // 调用现有的MetaHudiTableManager方法插入数据
            boolean success = metaManager.insertTableMeta(
                    request.getId(),
                    request.getSchema(),
                    0, // 默认未上线
                    request.getPartitionExpr(),
                    request.getHoodieConfig(),
                    request.getTags(),
                    request.getDescription(),
                    request.getSourceDb(),
                    request.getSourceTable(),
                    request.getDbType(),
                    metaTablePath
            );
            
            if (success) {
                logger.info("成功创建表: {}", request.getId());
            } else {
                logger.error("创建表失败: {}", request.getId());
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("创建表异常: {}", request.getId(), e);
            throw new RuntimeException("创建表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean updateTable(String id, UpdateTableRequest request) {
        try {
            logger.info("更新表信息: {}", id);
            
            // 验证schema（如果提供）
            if (request.getSchema() != null) {
                validateTableSchema(request.getSchema());
            }
            
            // 获取现有记录
            Optional<MetaTableDTO> existingTable = getTableById(id);
            if (!existingTable.isPresent()) {
                throw new IllegalArgumentException("表ID " + id + " 不存在");
            }
            
            MetaTableDTO existing = existingTable.get();
            
            // 更新字段
            String newSchema = request.getSchema() != null ? request.getSchema() : existing.getSchema();
            Integer newStatus = request.getStatus() != null ? request.getStatus() : existing.getStatus();
            String newPartitionExpr = request.getPartitionExpr() != null ? request.getPartitionExpr() : existing.getPartitionExpr();
            String newTags = request.getTags() != null ? request.getTags() : existing.getTags();
            String newDescription = request.getDescription() != null ? request.getDescription() : existing.getDescription();
            String newSourceDb = request.getSourceDb() != null ? request.getSourceDb() : existing.getSourceDb();
            String newSourceTable = request.getSourceTable() != null ? request.getSourceTable() : existing.getSourceTable();
            String newDbType = request.getDbType() != null ? request.getDbType() : existing.getDbType();
            String newHoodieConfig = request.getHoodieConfig() != null ? request.getHoodieConfig() : existing.getHoodieConfig();
            
            // 调用MetaHudiTableManager更新数据
            boolean success = metaManager.insertTableMeta(
                    id,
                    newSchema,
                    newStatus,
                    newPartitionExpr,
                    newHoodieConfig,
                    newTags,
                    newDescription,
                    newSourceDb,
                    newSourceTable,
                    newDbType,
                    metaTablePath
            );
            
            if (success) {
                logger.info("成功更新表: {}", id);
            } else {
                logger.error("更新表失败: {}", id);
            }
            
            return success;
            
        } catch (Exception e) {
            logger.error("更新表异常: {}", id, e);
            throw new RuntimeException("更新表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean deleteTable(String id) {
        try {
            logger.info("删除表: {}", id);
            
            // 检查表是否存在
            if (!existsTableId(id)) {
                throw new IllegalArgumentException("表ID " + id + " 不存在");
            }

            // 设置cdc_delete_flag字段为1
            metaManager.updateTableMeta(id, "cdc_delete_flag", 1, metaTablePath);

            return true;
            
        } catch (Exception e) {
            logger.error("删除表异常: {}", id, e);
            throw new RuntimeException("删除表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public PageResult<MetaTableDTO> searchTables(SearchCriteria criteria, int page, int size) {
        try {
            logger.info("搜索表，条件: {}", criteria);
            
            Dataset<Row> df = sparkSession.read().format("hudi").load(metaTablePath);
            
            // 应用搜索条件
            if (criteria.getKeyword() != null && !criteria.getKeyword().trim().isEmpty()) {
                String keyword = criteria.getKeyword().trim();
                df = df.filter(
                        df.col("id").contains(keyword)
                                .or(df.col("description").contains(keyword))
                                .or(df.col("source_table").contains(keyword))
                );
            }
            
            if (criteria.getStatus() != null) {
                df = df.filter(df.col("status").equalTo(criteria.getStatus()));
            }
            
            if (criteria.getSourceDb() != null && !criteria.getSourceDb().trim().isEmpty()) {
                df = df.filter(df.col("source_db").equalTo(criteria.getSourceDb()));
            }
            
            if (criteria.getDbType() != null && !criteria.getDbType().trim().isEmpty()) {
                df = df.filter(df.col("db_type").equalTo(criteria.getDbType()));
            }
            
            if (criteria.getIsPartitioned() != null) {
                df = df.filter(df.col("is_partitioned").equalTo(criteria.getIsPartitioned()));
            }
            
            // 时间范围过滤
            if (criteria.getCreateTimeStart() != null) {
                String startTime = criteria.getCreateTimeStart().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                df = df.filter(df.col("create_time").geq(startTime));
            }
            
            if (criteria.getCreateTimeEnd() != null) {
                String endTime = criteria.getCreateTimeEnd().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                df = df.filter(df.col("create_time").leq(endTime));
            }
            
            long total = df.count();
            
            // 分页处理
            long offset = (long) page * size;
            Dataset<Row> pagedDF = df
                    .orderBy(df.col("update_time").desc())
                    .limit(size)
                    .offset((int) offset);
            
            List<MetaTableDTO> tables = convertDatasetToMetaTableDTO(pagedDF);
            
            logger.info("搜索完成，找到 {} 条记录", total);
            return new PageResult<>(tables, total, page, size);
            
        } catch (Exception e) {
            logger.error("搜索表失败", e);
            throw new RuntimeException("搜索表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public BatchOperationResult batchOperation(BatchOperationRequest request) {
        logger.info("批量操作: {}, 影响表数: {}", request.getOperation(), request.getIds().size());
        
        switch (request.getOperation().toLowerCase()) {
            case "delete":
                return batchDeleteTables(request.getIds());
            case "online":
                return batchOnlineTables(request.getIds());
            case "offline":
                return batchOfflineTables(request.getIds());
            case "export":
                return exportTables(request.getIds());
            default:
                throw new IllegalArgumentException("不支持的批量操作类型: " + request.getOperation());
        }
    }
    
    @Override
    public TableStatusStats getTableStatusStats() {
        try {
            logger.info("获取表状态统计");
            
            Dataset<Row> df = sparkSession.read().format("hudi").load(metaTablePath);
            long total = df.count();
            long online = df.filter(df.col("status").equalTo(1)).count();
            long offline = df.filter(df.col("status").equalTo(0)).count();
            long partitioned = df.filter(df.col("is_partitioned").equalTo(true)).count();
            long nonPartitioned = df.filter(df.col("is_partitioned").equalTo(false)).count();
            
            return new TableStatusStats(total, online, offline, partitioned, nonPartitioned);
            
        } catch (Exception e) {
            logger.error("获取表状态统计失败", e);
            throw new RuntimeException("获取表状态统计失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean onlineTable(String id) {
        try {
            logger.info("表上线: {}", id);
            return metaManager.setTableOnline(id, metaTablePath);
        } catch (Exception e) {
            logger.error("表上线失败: {}", id, e);
            throw new RuntimeException("表上线失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean offlineTable(String id) {
        try {
            logger.info("表下线: {}", id);
            return metaManager.setTableOffline(id, metaTablePath);
        } catch (Exception e) {
            logger.error("表下线失败: {}", id, e);
            throw new RuntimeException("表下线失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public boolean validateTableSchema(String schema) {
        try {
            logger.debug("验证表schema: {}", schema.substring(0, Math.min(100, schema.length())));
            
            // 使用Jackson验证JSON格式
            objectMapper.readTree(schema);
            
            // 可以添加更多的schema验证逻辑
            return true;
            
        } catch (Exception e) {
            logger.error("Schema验证失败", e);
            throw new IllegalArgumentException("Schema格式无效: " + e.getMessage(), e);
        }
    }
    
    @Override
    public List<String> getSupportedDbTypes() {
        return new ArrayList<>(SUPPORTED_DB_TYPES);
    }
    
    @Override
    public boolean existsTableId(String id) {
        try {
            Optional<MetaTableDTO> table = getTableById(id);
            return table.isPresent();
        } catch (Exception e) {
            logger.error("检查表ID是否存在失败: {}", id, e);
            return false;
        }
    }
    
    @Override
    public BatchOperationResult batchDeleteTables(List<String> ids) {
        List<String> errors = new ArrayList<>();
        int success = 0;
        int failed = 0;
        
        for (String id : ids) {
            try {
                if (deleteTable(id)) {
                    success++;
                } else {
                    failed++;
                    errors.add("删除表失败: " + id);
                }
            } catch (Exception e) {
                failed++;
                errors.add("删除表异常: " + id + " - " + e.getMessage());
            }
        }
        
        return new BatchOperationResult(success, failed, ids.size(), errors);
    }
    
    @Override
    public BatchOperationResult batchOnlineTables(List<String> ids) {
        List<String> errors = new ArrayList<>();
        int success = 0;
        int failed = 0;
        
        for (String id : ids) {
            try {
                if (onlineTable(id)) {
                    success++;
                } else {
                    failed++;
                    errors.add("表上线失败: " + id);
                }
            } catch (Exception e) {
                failed++;
                errors.add("表上线异常: " + id + " - " + e.getMessage());
            }
        }
        
        return new BatchOperationResult(success, failed, ids.size(), errors);
    }
    
    @Override
    public BatchOperationResult batchOfflineTables(List<String> ids) {
        List<String> errors = new ArrayList<>();
        int success = 0;
        int failed = 0;
        
        for (String id : ids) {
            try {
                if (offlineTable(id)) {
                    success++;
                } else {
                    failed++;
                    errors.add("表下线失败: " + id);
                }
            } catch (Exception e) {
                failed++;
                errors.add("表下线异常: " + id + " - " + e.getMessage());
            }
        }
        
        return new BatchOperationResult(success, failed, ids.size(), errors);
    }
    
    @Override
    public BatchOperationResult exportTables(List<String> ids) {
        // 导出功能的具体实现
        logger.info("导出表: {}", ids);
        // 这里可以实现具体的导出逻辑
        return new BatchOperationResult(ids.size(), 0, ids.size(), new ArrayList<>());
    }
    
    @Override
    public BatchOperationResult exportSearchResults(SearchCriteria criteria) {
        try {
            PageResult<MetaTableDTO> result = searchTables(criteria, 0, Integer.MAX_VALUE);
            List<String> ids = result.getData().stream()
                    .map(MetaTableDTO::getId)
                    .collect(Collectors.toList());
            return exportTables(ids);
        } catch (Exception e) {
            logger.error("导出搜索结果失败", e);
            throw new RuntimeException("导出搜索结果失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public Optional<String> getTableSchema(String id) {
        Optional<MetaTableDTO> table = getTableById(id);
        return table.map(MetaTableDTO::getSchema);
    }
    
    @Override
    public List<Object> getTableHistory(String id) {
        // 获取表历史记录的具体实现
        logger.info("获取表历史记录: {}", id);
        // 这里可以实现具体的历史记录查询逻辑
        return new ArrayList<>();
    }
    
    @Override
    public boolean copyTable(String id, String newId) {
        try {
            Optional<MetaTableDTO> sourceTable = getTableById(id);
            if (!sourceTable.isPresent()) {
                throw new IllegalArgumentException("源表不存在: " + id);
            }
            
            if (existsTableId(newId)) {
                throw new IllegalArgumentException("目标表ID已存在: " + newId);
            }
            
            MetaTableDTO source = sourceTable.get();
            CreateTableRequest request = new CreateTableRequest();
            request.setId(newId);
            request.setSchema(source.getSchema());
            request.setIsPartitioned(source.getIsPartitioned());
            request.setPartitionExpr(source.getPartitionExpr());
            request.setTags(source.getTags());
            request.setDescription("复制自: " + id + " - " + source.getDescription());
            request.setSourceDb(source.getSourceDb());
            request.setSourceTable(source.getSourceTable());
            request.setDbType(source.getDbType());
            request.setHoodieConfig(source.getHoodieConfig());
            
            return createTable(request);
            
        } catch (Exception e) {
            logger.error("复制表失败: {} -> {}", id, newId, e);
            throw new RuntimeException("复制表失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public Object getTableUsageStats(String id) {
        // 获取表使用统计的具体实现
        logger.info("获取表使用统计: {}", id);
        Map<String, Object> stats = new HashMap<>();
        stats.put("tableId", id);
        stats.put("lastAccessTime", LocalDateTime.now());
        stats.put("accessCount", 0);
        return stats;
    }
    
    /**
     * 将Spark Dataset转换为MetaTableDTO列表
     */
    private List<MetaTableDTO> convertDatasetToMetaTableDTO(Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        List<MetaTableDTO> result = new ArrayList<>();
        
        for (Row row : rows) {
            MetaTableDTO dto = new MetaTableDTO();
            dto.setId(row.getAs("id"));
            dto.setSchema(row.getAs("schema"));
            dto.setStatus(row.getAs("status"));
            dto.setIsPartitioned(row.getAs("is_partitioned"));
            dto.setPartitionExpr(row.getAs("partition_expr"));
            dto.setTags(row.getAs("tags"));
            dto.setDescription(row.getAs("description"));
            dto.setSourceDb(row.getAs("source_db"));
            dto.setSourceTable(row.getAs("source_table"));
            dto.setDbType(row.getAs("db_type"));
            dto.setHoodieConfig(row.getAs("hoodie_config"));
            
            // 处理时间字段
            try {
                Object createTimeObj = row.getAs("create_time");
                if (createTimeObj != null) {
                    if (createTimeObj instanceof java.sql.Timestamp) {
                        dto.setCreateTime(((java.sql.Timestamp) createTimeObj).toLocalDateTime());
                    } else if (createTimeObj instanceof String) {
                        dto.setCreateTime(LocalDateTime.parse((String) createTimeObj, 
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    }
                }
                
                Object updateTimeObj = row.getAs("update_time");
                if (updateTimeObj != null) {
                    if (updateTimeObj instanceof java.sql.Timestamp) {
                        dto.setUpdateTime(((java.sql.Timestamp) updateTimeObj).toLocalDateTime());
                    } else if (updateTimeObj instanceof String) {
                        dto.setUpdateTime(LocalDateTime.parse((String) updateTimeObj, 
                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
                    }
                }
            } catch (Exception e) {
                logger.warn("解析时间字段失败: {}", e.getMessage());
            }
            
            result.add(dto);
        }
        
        return result;
    }

    @Override
    public PageResult<MetaTableDTO> getDeletedTables(int page, int size) {
        try {
            logger.info("获取已删除表，页码: {}, 每页大小: {}", page, size);
            Dataset<Row> df = sparkSession.read().format("hudi").load(metaTablePath);
            long total = df.count();
            long offset = (long) page * size;
            Dataset<Row> pagedDF = df.filter(df.col("cdc_delete_flag").equalTo(1))
                    .orderBy(df.col("update_time").desc())
                    .limit(size)
                    .offset((int) offset);
            List<MetaTableDTO> tables = convertDatasetToMetaTableDTO(pagedDF);
            return new PageResult<>(tables, total, page, size);
        } catch (Exception e) {
            logger.error("获取已删除表失败", e);
            throw new RuntimeException("获取已删除表失败: " + e.getMessage(), e);
        }
    }

    @Override
    public List<Object> getSystemTables() {
        return Collections.emptyList();
    }

    @Override
    public Object getSystemTableStats() {
        return null;
    }

    @Override
    public boolean restoreTable(String id) {
        return false;
    }
} 