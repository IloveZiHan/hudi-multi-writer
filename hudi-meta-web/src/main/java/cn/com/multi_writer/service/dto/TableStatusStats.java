package cn.com.multi_writer.service.dto;

import java.util.Objects;

/**
 * 表状态统计对象
 */
public class TableStatusStats {
    
    private long total;
    
    private long online;
    
    private long offline;
    
    private long partitioned;
    
    private long nonPartitioned;
    
    // 默认构造函数
    public TableStatusStats() {
    }
    
    // 完整构造函数
    public TableStatusStats(long total, long online, long offline, long partitioned, long nonPartitioned) {
        this.total = total;
        this.online = online;
        this.offline = offline;
        this.partitioned = partitioned;
        this.nonPartitioned = nonPartitioned;
    }
    
    // Getters and Setters
    public long getTotal() {
        return total;
    }
    
    public void setTotal(long total) {
        this.total = total;
    }
    
    public long getOnline() {
        return online;
    }
    
    public void setOnline(long online) {
        this.online = online;
    }
    
    public long getOffline() {
        return offline;
    }
    
    public void setOffline(long offline) {
        this.offline = offline;
    }
    
    public long getPartitioned() {
        return partitioned;
    }
    
    public void setPartitioned(long partitioned) {
        this.partitioned = partitioned;
    }
    
    public long getNonPartitioned() {
        return nonPartitioned;
    }
    
    public void setNonPartitioned(long nonPartitioned) {
        this.nonPartitioned = nonPartitioned;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TableStatusStats that = (TableStatusStats) o;
        return total == that.total &&
                online == that.online &&
                offline == that.offline &&
                partitioned == that.partitioned &&
                nonPartitioned == that.nonPartitioned;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(total, online, offline, partitioned, nonPartitioned);
    }
    
    @Override
    public String toString() {
        return "TableStatusStats{" +
                "total=" + total +
                ", online=" + online +
                ", offline=" + offline +
                ", partitioned=" + partitioned +
                ", nonPartitioned=" + nonPartitioned +
                '}';
    }
} 