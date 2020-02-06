package de.flinkmath;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExerciseInputFormat implements InputFormat<HistoryObjects, SheetInputSplit1> {
    public static final ConfigOption<String> CONFIG_FILE_NAME_OPTION = ConfigOptions.key("de.flinkmath.filename").defaultValue("");
    public static final ConfigOption<String> CONFIG_PROCESSING_TIME = ConfigOptions.key("de.flinkmath.processingtime").defaultValue("1");

    private String configFileName;
    private String processingTime;
    private Iterator<HistoryObjects> historyIterator;
    private SheetInputSplit1 sheetInputSplit1;

    @Override
    public void configure(Configuration configuration) {
        this.configFileName = configuration.getString(CONFIG_FILE_NAME_OPTION);
        this.processingTime = configuration.getString(CONFIG_PROCESSING_TIME);
        log("Received config file name " + this.configFileName + " and set processing time per update to " + this.processingTime + "ms.");
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) throws IOException {
        // not available
        return null;
    }

    @Override
    public SheetInputSplit1[] createInputSplits(int i) throws IOException {
        long startTime = System.nanoTime();
        List<String> sheetConfigStrings = readFile(configFileName);
        SheetInputSplit1[] inputSplits = IntStream
                .range(0, sheetConfigStrings.size())
                .mapToObj((int index) -> {
                    String sheetConfigString = sheetConfigStrings.get(index);
                    String[] sheetConfigSplit = sheetConfigString.split(" ");
                    return new SheetInputSplit1(index, sheetConfigSplit[0], sheetConfigSplit[1]); })
                .toArray(SheetInputSplit1[]::new);
        long endTime = System.nanoTime();
        long timeElapsed = endTime - startTime;
        log("Created input splits in: " + timeElapsed / 1000000 + "ms");
        return inputSplits;
    }

    /*
    @Override
    public SheetInputSplit5[] createInputSplits(int i) throws IOException {
        log("Creating input splits");
        List<String> sheetConfigStrings = readFile(configFileName);
        return IntStream.range(0, sheetConfigStrings.size())
                .mapToObj((int index) -> {
                    String sheetConfigString = sheetConfigStrings.get(index);
                    String[] sheetConfigSplit = sheetConfigString.split(" ");
                    return new SheetInputSplit5(
                            index,
                            sheetConfigSplit[0],
                            sheetConfigSplit[1],
                            sheetConfigSplit[2],
                            sheetConfigSplit[3],
                            sheetConfigSplit[4],
                            sheetConfigSplit[5]);
                }).toArray(SheetInputSplit5[]::new);
    }

    @Override
    public SheetInputSplit10[] createInputSplits(int i) throws IOException {
        log("Creating input splits");
        List<String> sheetConfigStrings = readFile(configFileName);
        return IntStream.range(0, sheetConfigStrings.size())
                .mapToObj((int index) -> {
                    String sheetConfigString = sheetConfigStrings.get(index);
                    String[] sheetConfigSplit = sheetConfigString.split(" ");
                    return new SheetInputSplit10(
                            index,
                            sheetConfigSplit[0],
                            sheetConfigSplit[1],
                            sheetConfigSplit[2],
                            sheetConfigSplit[3],
                            sheetConfigSplit[4],
                            sheetConfigSplit[5],
                            sheetConfigSplit[6],
                            sheetConfigSplit[7],
                            sheetConfigSplit[8],
                            sheetConfigSplit[9],
                            sheetConfigSplit[10]);
                }).toArray(SheetInputSplit10[]::new);
    }
    */
    @Override
    public InputSplitAssigner getInputSplitAssigner(SheetInputSplit1[] sheetInputSplits) {
        log("InputSplitAssigner requested");
        return new SheetInputSplitAssigner(sheetInputSplits);
    }

    @Override
    public void open(SheetInputSplit1 sheetInputSplit1) throws IOException {
        this.sheetInputSplit1 = sheetInputSplit1;
        long startTime1 = System.nanoTime();
        String[] ncidBatch = sheetInputSplit1.getNcid1().split("#");
        long endTime1 = System.nanoTime();
        long timeElapsed = endTime1 - startTime1;
        Connection c = null;
        Statement stmt = null;
        try {
            long startTime2 = System.nanoTime();
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5433/history-1000000",
                            "postgres", "");
            long endTime2 = System.nanoTime();
            long timeElapsed2 = endTime2 - startTime2;

            long startTime3 = System.nanoTime();
            if(ncidBatch.length > 0) {
                for (String ncid: ncidBatch) {
                    stmt = c.createStatement();
                    // History Object
                    HistoryObjects obj = new HistoryObjects();
                    String sqlObject = "Select * from public.objects WHERE nc_id = '" + ncid + "';";
                    ResultSet rs = stmt.executeQuery(sqlObject);
                    while (rs.next()) {
                        obj.setNcid(rs.getString(1));
                        obj.setUpdates(rs.getInt(2));
                        obj.setUpdateGroups(rs.getInt(3));
                        obj.setCheckpoints(rs.getInt(4));
                        obj.setCheckpointList(new ArrayList<>());
                        obj.setUpdateList(new ArrayList<>());
                    }
                    String sqlCheckpoints = "Select * from public.checkpoints WHERE nc_id = '" + ncid + "' AND last_update IS NULL;";
                    rs = stmt.executeQuery(sqlCheckpoints);
                    while (rs.next()) {
                        Checkpoint checkpoint = new Checkpoint(
                                rs.getString(2),
                                rs.getInt(1),
                                rs.getInt(3),
                                rs.getTimestamp(4).toLocalDateTime(),
                                rs.getString(5),
                                rs.getString(6),
                                rs.getString(7),
                                rs.getString(8),
                                rs.getString(9),
                                rs.getString(10),
                                rs.getString(11),
                                rs.getString(12),
                                rs.getString(13),
                                rs.getString(14),
                                rs.getString(15),
                                rs.getString(16),
                                rs.getString(17),
                                rs.getString(18),
                                rs.getString(19),
                                rs.getString(20),
                                rs.getString(21),
                                rs.getString(22),
                                rs.getString(23),
                                rs.getString(24),
                                rs.getString(25),
                                rs.getString(26),
                                rs.getString(27),
                                rs.getString(28),
                                rs.getString(29),
                                rs.getString(30),
                                rs.getString(31),
                                rs.getString(32),
                                rs.getString(33),
                                rs.getString(34),
                                rs.getString(35),
                                rs.getString(36),
                                rs.getString(37),
                                rs.getString(38),
                                rs.getString(39),
                                rs.getString(40),
                                rs.getString(41));
                        obj.addCheckPointList(checkpoint);
                    }
                    String sqlUpdates = "Select * from public.updates WHERE nc_id = '" + ncid + "';";
                    rs = stmt.executeQuery(sqlUpdates);
                    while (rs.next()) {
                        Update update = new Update(
                                rs.getString(3),
                                rs.getInt(1),
                                rs.getInt(2),
                                rs.getTimestamp(4).toLocalDateTime(),
                                rs.getString(5),
                                rs.getString(6)
                        );
                        obj.addUpdateList(update);
                    }
                    // Time-out the process to simulate more realistic calculation pattern. This should account
                    // for the fact that histories of different size take a different amount of time to process.
                    TimeUnit.MILLISECONDS.sleep(Math.round(obj.getUpdates()*Float.parseFloat(this.processingTime)));
                    stmt.close();
                }
            }
            long endTime3 = System.nanoTime();
            long timeElapsed3 = endTime3 - startTime3;
            System.out.println("Finished processing Batch. Stats(Prepare Batch: " + timeElapsed / 1000000 + "ms, Processed batch in: " + timeElapsed3 / 1000000 + "ms, DB connection time: " + timeElapsed2 / 1000000 + "ms.)");
            c.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return true;
    }

    @Override
    public HistoryObjects nextRecord(HistoryObjects toReuse) throws IOException {
        log("toReuse" + sheetInputSplit1 + " is " + toReuse.toString());
        HistoryObjects historyObjects = this.historyIterator.next();
        log("Next record for " + sheetInputSplit1 + " is " + historyObjects.toString());
        return historyObjects;
    }

    @Override
    public void close() throws IOException {
        log("Sheet " + sheetInputSplit1 + " closed");
        // not needed
    }

    private void log(String s) {
        System.out.println("ExerciseInputFormat: "+s);
    }

    private List<String> readFile(String fileName) throws FileNotFoundException {
        BufferedReader reader = new BufferedReader(new FileReader(new File(fileName)));
        return reader.lines().collect(Collectors.toList());
    }
}
