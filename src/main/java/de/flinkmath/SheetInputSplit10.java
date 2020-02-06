package de.flinkmath;

import org.apache.flink.core.io.InputSplit;

public class SheetInputSplit10 implements InputSplit {

    private int splitNumber;
    private String hostName;
    private String ncid1;
    private String ncid2;
    private String ncid3;
    private String ncid4;
    private String ncid5;
    private String ncid6;
    private String ncid7;
    private String ncid8;
    private String ncid9;
    private String ncid10;


    public SheetInputSplit10(int splitNumber,
                             String hostName,
                             String ncid1,
                             String ncid2,
                             String ncid3,
                             String ncid4,
                             String ncid5,
                             String ncid6,
                             String ncid7,
                             String ncid8,
                             String ncid9,
                             String ncid10) {
        this.splitNumber = splitNumber;
        this.hostName = hostName;
        this.ncid1 = ncid1;
        this.ncid2 = ncid2;
        this.ncid3 = ncid3;
        this.ncid4 = ncid4;
        this.ncid5 = ncid5;
        this.ncid6 = ncid6;
        this.ncid7 = ncid7;
        this.ncid8 = ncid8;
        this.ncid9 = ncid9;
        this.ncid10 = ncid10;
    }

    @Override
    public int getSplitNumber() {
        return this.splitNumber;
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

    public String getNcid2() {
        return ncid2;
    }

    public void setNcid2(String ncid2) {
        this.ncid2 = ncid2;
    }

    public String getNcid3() {
        return ncid3;
    }

    public void setNcid3(String ncid3) {
        this.ncid3 = ncid3;
    }

    public String getNcid4() {
        return ncid4;
    }

    public void setNcid4(String ncid4) {
        this.ncid4 = ncid4;
    }

    public String getNcid5() {
        return ncid5;
    }

    public void setNcid5(String ncid5) {
        this.ncid5 = ncid5;
    }

    public String getNcid6() {
        return ncid6;
    }

    public void setNcid6(String ncid6) {
        this.ncid6 = ncid6;
    }

    public String getNcid7() {
        return ncid7;
    }

    public void setNcid7(String ncid7) {
        this.ncid7 = ncid7;
    }

    public String getNcid8() {
        return ncid8;
    }

    public void setNcid8(String ncid8) {
        this.ncid8 = ncid8;
    }

    public String getNcid9() {
        return ncid9;
    }

    public void setNcid9(String ncid9) {
        this.ncid9 = ncid9;
    }

    public String getNcid10() {
        return ncid10;
    }

    public void setNcid10(String ncid10) {
        this.ncid10 = ncid10;
    }
}
