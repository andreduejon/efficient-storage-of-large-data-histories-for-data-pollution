package de.flinkmath;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.Optional;

public class Exercise {

    private String hostName;
    private String sheetName;
    private String ncid;
    private String task;

    public Exercise(String hostName, String sheetName, String ncid, String task) {
        this.hostName = hostName;
        this.sheetName = sheetName;
        this.ncid = ncid;
        this.task = task;
    }

    public Solution solve() {
        return  new Solution(ncid);
    }

    private double calculateMultiplication(String multiplication) {
        Optional<Double> result = Lists.newArrayList(multiplication.split("\\*")).stream()
                .map(String::trim)
                .map(Double::parseDouble)
                .reduce((Double x, Double y) -> x * y);
        if (result.isPresent()) {
            return result.get();
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "Exercise {hostName: " + hostName + "}";
    }
}
