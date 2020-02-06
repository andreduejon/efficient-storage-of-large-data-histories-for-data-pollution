package de.flinkmath;

import org.apache.flink.core.io.InputSplit;

public class SheetInputSplit1 implements InputSplit {

    private int splitNumber;
    private String hostName;
    private String ncid1;

    public SheetInputSplit1(int splitNumber, String hostName, String ncid1) {
        this.splitNumber = splitNumber;
        this.hostName = hostName;
        this.ncid1 = ncid1;
    }

    public void setSplitNumber(int splitNumber) {
        this.splitNumber = splitNumber;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getNcid1() {
        return ncid1;
    }

    public void setNcid1(String ncid1) {
        this.ncid1 = ncid1;
    }

    @Override
    public int getSplitNumber() {
        return this.splitNumber;
    }
}
