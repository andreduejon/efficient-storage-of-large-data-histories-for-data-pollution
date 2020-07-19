package de.flink;

import java.util.List;

public class HistoryObjects
{
    private String ncid;
    private int updates;
    private int updateGroups;
    private int checkpoints;
    private List<Checkpoint> checkpointList;
    private List<Update> updateList;

    public HistoryObjects() {
    }

    public HistoryObjects(String ncid, int updates, int updateGroups, int checkpoints) {
        this.ncid = ncid;
        this.updates = updates;
        this.updateGroups = updateGroups;
        this.checkpoints = checkpoints;
    }

    public String getNcid() {
        return ncid;
    }

    public void setNcid(String ncid) {
        this.ncid = ncid;
    }

    public int getUpdates() {
        return updates;
    }
    public void setUpdates(int updates) {
        this.updates = updates;
    }

    public int getUpdateGroups() {
        return updateGroups;
    }
    public void setUpdateGroups(int updateGroups) {
        this.updateGroups = updateGroups;
    }

    public int getCheckpoints() {
        return checkpoints;
    }
    public void setCheckpoints(int checkpoints) {
        this.checkpoints = checkpoints;
    }

    public List<Checkpoint> getCheckpointList() {
        return checkpointList;
    }

    public void setCheckpointList(List<Checkpoint> checkpointList) {
        this.checkpointList = checkpointList;
    }

    public List<Update> getUpdateList() {
        return updateList;
    }

    public void setUpdateList(List<Update> updateList) {
        this.updateList = updateList;
    }

    public int sizeUpdateList() {
        return getUpdateList().size();
    }

    public boolean sizeCheckpointList() {
        return getCheckpointList().isEmpty();
    }


    public boolean addUpdateList(Update update) {
        return getUpdateList().add(update);
    }

    public boolean addCheckPointList(Checkpoint checkpoint) {
        return getCheckpointList().add(checkpoint);
    }

    public int sizeCheckPointList() {
        return getCheckpointList().size();
    }

    public boolean isEmptyCheckPointList() {
        return getCheckpointList().isEmpty();
    }

    public Solution solve() {
        return  new Solution(ncid);
    }

    @Override
    public String toString() {
        return "HistoryObjects{" +
                "ncid='" + ncid + '\'' +
                ", updates=" + updates +
                ", updateGroups=" + updateGroups +
                ", checkpoints=" + checkpoints +
                ", checkpointList=" + checkpointList +
                ", updateList=" + updateList +
                '}';
    }
}
