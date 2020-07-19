package de.flink;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class Main {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Configuration inputConf = new Configuration();
        if(args[2].equals("-u")) {
            inputConf.setString(ExerciseInputFormat.CONFIG_PROCESSING_TIME, args[3]);
        }
        if(args[0].equals("-f")) {
            inputConf.setString(ExerciseInputFormat.CONFIG_FILE_NAME_OPTION, args[1]);
            env.createInput(new ExerciseInputFormat()).withParameters(inputConf)
                    .map(HistoryObjects::solve)
                    .output(new SolutionOutputFormat());
            env.execute();
        } else {
            System.out.println("No file containing the input splits was specified. Use -f parameter and specify the file path.");
        }

    }
}