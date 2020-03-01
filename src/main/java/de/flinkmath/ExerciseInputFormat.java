package de.flinkmath;

import com.datastax.driver.core.*;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.*;
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
            Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
            Session session = cluster.connect("history");
            long endTime2 = System.nanoTime();
            m1 = "Connection: " + (endTime2-startTime2) + "ns";

            long startTime3 = System.nanoTime();
            if(ncidBatch.length > 0) {
                for (String ncid: ncidBatch) {
                    long q1 = System.nanoTime();
                    ResultSet rs = session.execute("select * from data where ncid = '" + ncid + "'" );
                    long q1e = System.nanoTime();
                    m2 = "Query: " + (q1e-q1) + "ns";
                    if(rs != null) {
                        long p1 = System.nanoTime();
                        HistoryObjects obj = new HistoryObjects();
                        obj.setNcid(ncid);
                        obj.setCheckpointList(new ArrayList<>());
                        obj.setUpdateList(new ArrayList<>());
                        Row row = rs.one();
                        Checkpoint checkpoint = new Checkpoint(
                                ncid,
                                row.getInt("event_id"),
                                null,
                                LocalDateTime.ofInstant(row.getTimestamp("timestamp").toInstant(), ZoneId.systemDefault()),
                                row.getString("county_id"),
                                row.getString("county_desc"),
                                row.getString("last_name"),
                                row.getString("first_name"),
                                row.getString("midl_name"),
                                row.getString("house_num"),
                                row.getString("street_dir"),
                                row.getString("street_name"),
                                row.getString("res_city_desc"),
                                row.getString("state_cd"),
                                row.getString("zip_code"),
                                row.getString("area_cd"),
                                row.getString("phone_num"),
                                row.getString("race_code"),
                                row.getString("race_desc"),
                                row.getString("ethnic_code"),
                                row.getString("ethnic_desc"),
                                row.getString("party_cd"),
                                row.getString("party_desc"),
                                row.getString("sex_code"),
                                row.getString("sex"),
                                row.getString("age"),
                                row.getString("age_group"),
                                row.getString("name_prefx_cd"),
                                row.getString("name_sufx_cd"),
                                row.getString("half_code"),
                                row.getString("street_type_cd"),
                                row.getString("street_sufx_cd"),
                                row.getString("unit_designator"),
                                row.getString("unit_num"),
                                row.getString("mail_addr1"),
                                row.getString("mail_addr2"),
                                row.getString("mail_addr3"),
                                row.getString("mail_addr4"),
                                row.getString("mail_city"),
                                row.getString("mail_state"),
                                row.getString("mail_zipcode")
                        );
                        obj.addCheckPointList(checkpoint);

                        for(Row r : rs) {
                            if(r.getString("event_type").equals("update")) {
                                Update update = new Update(
                                        ncid,
                                        r.getInt("event_id"),
                                        r.getInt("event_id"),
                                        LocalDateTime.ofInstant(r.getTimestamp("timestamp").toInstant(), ZoneId.systemDefault()),
                                        r.getString("attribute"),
                                        r.getString("value")
                                );
                                obj.addUpdateList(update);
                            }
                        }
                        long p1e = System.nanoTime();
                        m3 = "Processing: " + (p1e-p1) + "ns";

                        // Time-out the process to simulate more realistic calculation pattern. This should account
                        // for the fact that histories of different size take a different amount of time to process.
                        long timeout = Math.round(obj.getUpdateList().size()*Float.parseFloat(this.processingTime));
                        m4 = "Timeout: " + timeout;
                        m5 = "Updates: " + obj.getUpdateList().size();
                        TimeUnit.MILLISECONDS.sleep(timeout);
                    }
                }
            }
            cluster.close();
            long endTime3 = System.nanoTime();
            long timeElapsed3 = endTime3 - startTime3;
            System.out.println("Complete: " + timeElapsed3 / 1000000 + "ms - " + m1 + " - " + m2 + " - " + m3 + " - " + m4 + " - " + m5);
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
