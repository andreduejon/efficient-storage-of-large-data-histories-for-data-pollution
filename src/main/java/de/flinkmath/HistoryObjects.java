package de.flinkmath;

public class HistoryObjects
{
    private String ncid;
    private int updates;
    private int updateGroups;
    private int checkpoints;

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
                '}';
    }
}
