package de.flink;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;

public class SolutionOutputFormat implements OutputFormat<Solution> {
    private int taskNumber;

    @Override
    public void configure(Configuration configuration) {
        // not needed
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        log("Opening for taskNumber " + taskNumber + " " + numTasks + " parallel tasks");
        this.taskNumber = taskNumber;
    }

    @Override
    public void writeRecord(Solution solution) throws IOException {
        log("Task " + taskNumber + "processed " + solution.getNcid());
    }

    @Override
    public void close() throws IOException {
        log("Closing task" + taskNumber);
    }

    private void log(String s) {
        System.out.println("SolutionOutputFormat: " + s);
    }
}
