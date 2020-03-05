package de.flinkmath;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.*;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
        String m1 = "";
        String m2 = "";
        String m3 = "";
        String m4 = "";
        String m5 = "";
        this.sheetInputSplit1 = sheetInputSplit1;
        String[] ncidBatch = sheetInputSplit1.getNcid1().split("#");
        try {
            long startTime2 = System.nanoTime();
            Class.forName("org.postgresql.Driver");
            Connection c = null;
            Statement stmt = null;
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres",
                            "postgres", "postgres");
            long endTime2 = System.nanoTime();
            m1 = "Connection: " + (endTime2 - startTime2) + "ns";

            long startTime3 = System.nanoTime();
            if (ncidBatch.length > 0) {
                for (String ncid : ncidBatch) {
                    long p1 = System.nanoTime();
                    long q1 = System.nanoTime();
                    stmt = c.createStatement();
                    // History Object
                    HistoryObjects obj = new HistoryObjects();
                    String sqlObject = "Select * from public.objects WHERE nc_id = '" + ncid + "';";
                    ResultSet rs = stmt.executeQuery(sqlObject);
                    while (rs.next()) {
                        obj.setNcid(rs.getString("nc_id"));
                        obj.setUpdates(rs.getInt("updates"));
                        obj.setUpdateGroups(rs.getInt("update_groups"));
                        obj.setCheckpoints(rs.getInt("checkpoints"));
                        obj.setCheckpointList(new ArrayList<>());
                        obj.setUpdateList(new ArrayList<>());
                    }
                    String sqlUpdates = "Select * from public.updates WHERE nc_id = '" + ncid + "';";
                    rs = stmt.executeQuery(sqlUpdates);
                    while (rs.next()) {
                        Update update = new Update(
                                rs.getString("nc_id"),
                                rs.getInt("update_id"),
                                rs.getInt("update_group"),
                                rs.getTimestamp("timestamp").toLocalDateTime(),
                                rs.getString("attribute"),
                                rs.getString("value")
                        );
                        obj.addUpdateList(update);
                    }
                    String sqlCheckpoints = "Select * from public.checkpoints WHERE nc_id = '" + ncid + "' AND last_update IS NULL;";
                    rs = stmt.executeQuery(sqlCheckpoints);
                    while (rs.next()) {
                        Checkpoint checkpoint = new Checkpoint(
                                rs.getString("nc_id"),
                                rs.getInt("checkpoint_id"),
                                rs.getInt("last_update"),
                                rs.getTimestamp("timestamp").toLocalDateTime(),
                                rs.getString("county_id"),
                                rs.getString("county_desc"),
                                rs.getString("last_name"),
                                rs.getString("first_name"),
                                rs.getString("midl_name"),
                                rs.getString("house_num"),
                                rs.getString("street_dir"),
                                rs.getString("street_name"),
                                rs.getString("res_city_desc"),
                                rs.getString("state_cd"),
                                rs.getString("zip_code"),
                                rs.getString("area_cd"),
                                rs.getString("phone_num"),
                                rs.getString("race_code"),
                                rs.getString("race_desc"),
                                rs.getString("ethnic_code"),
                                rs.getString("ethnic_desc"),
                                rs.getString("party_cd"),
                                rs.getString("party_desc"),
                                rs.getString("sex_code"),
                                rs.getString("sex"),
                                rs.getString("age"),
                                rs.getString("birth_place"),
                                rs.getString("age_group"),
                                rs.getString("name_prefx_cd"),
                                rs.getString("name_sufx_cd"),
                                rs.getString("half_code"),
                                rs.getString("street_type_cd"),
                                rs.getString("street_sufx_cd"),
                                rs.getString("unit_designator"),
                                rs.getString("unit_num"),
                                rs.getString("mail_addr1"),
                                rs.getString("mail_addr2"),
                                rs.getString("mail_addr3"),
                                rs.getString("mail_addr4"),
                                rs.getString("mail_city"),
                                rs.getString("mail_state"));
                        obj.addCheckPointList(checkpoint);
                    }
                    long q1e = System.nanoTime();
                    m2 = "Query: " + (q1e - q1) + "ns";

                    long p1e = System.nanoTime();
                    m3 = "Processing: " + (p1e - p1) + "ns";

                    // Time-out the process to simulate more realistic calculation pattern. This should account
                    // for the fact that histories of different size take a different amount of time to process.
                    long timeout = Math.round(obj.getUpdateList().size() * Float.parseFloat(this.processingTime));
                    m4 = "Timeout: " + timeout;
                    m5 = "Updates: " + obj.getUpdateList().size();
                    TimeUnit.MILLISECONDS.sleep(timeout);
                }
            }
            c.close();
            long endTime3 = System.nanoTime();
            long timeElapsed3 = endTime3 - startTime3;
            System.out.println("Complete: " + timeElapsed3 / 1000000 + "ms - " + m1 + " - " + m2 + " - " + m3 + " - " + m4 + " - " + m5);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
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
