package de.flinkmath;

import java.time.LocalDateTime;

public class Update {
    private String ncid;
    private int updateId;
    private int updateGroup;
    private LocalDateTime timestamp;
    private String attribute;
    private String value;

    public Update(String ncid, int updateId, int updateGroup, LocalDateTime timestamp, String attribute, String value) {
        this.ncid = ncid;
        this.updateId = updateId;
        this.updateGroup = updateGroup;
        this.timestamp = timestamp;
        this.attribute = attribute;
        this.value = value;
    }

    public String getNcid() {
        return ncid;
    }

    public void setNcid(String ncid) {
        this.ncid = ncid;
    }

    public int getUpdateId() {
        return updateId;
    }

    public void setUpdateId(int updateId) {
        this.updateId = updateId;
    }

    public int getUpdateGroup() {
        return updateGroup;
    }

    public void setUpdateGroup(int updateGroup) {
        this.updateGroup = updateGroup;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Update{" +
                "ncid='" + ncid + '\'' +
                ", updateId=" + updateId +
                ", updateGroup=" + updateGroup +
                ", timestamp=" + timestamp +
                ", attribute='" + attribute + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
