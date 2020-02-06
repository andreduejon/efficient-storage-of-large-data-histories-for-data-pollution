package de.flinkmath;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration inputConf = new Configuration();
        inputConf.setString(ExerciseInputFormat.CONFIG_FILE_NAME_OPTION, "/home/hadoop/ncids/ncids-5-1000-16.txt");
        env.createInput(new ExerciseInputFormat()).withParameters(inputConf)
                .map(HistoryObjects::solve)
                .output(new SolutionOutputFormat());
        env.execute();
    }
}