package de.flinkmath;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.*;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExerciseInputFormat implements InputFormat<HistoryObjects, SheetInputSplit1> {
    public static final ConfigOption<String> CONFIG_OUTDATED_FREQUENCY = ConfigOptions.key("de.flinkmath.outdatedfrequency").defaultValue("10");
    public static final ConfigOption<String> CONFIG_FILE_NAME_OPTION = ConfigOptions.key("de.flinkmath.filename").defaultValue("");
    public static final ConfigOption<String> CONFIG_PROCESSING_TIME = ConfigOptions.key("de.flinkmath.processingtime").defaultValue("1");

    private String configFileName;
    private String outdatedFrequency;
    private String processingTime;
    private Iterator<HistoryObjects> historyIterator;
    private SheetInputSplit1 sheetInputSplit1;
    private String[] dates = {
            "2008-11-04",
            "2009-01-01",
            "2014-01-01", "2014-01-01",
            "2014-05-06", "2014-05-06",
            "2016-01-01", "2016-01-01", "2016-01-01",
            "2016-03-15", "2016-03-15", "2016-03-15",
            "2019-01-01", "2019-01-01", "2019-01-01", "2019-01-01",
            "2011-01-01",
            "2010-01-01",
            "2014-07-15", "2014-07-15",
            "2014-11-04", "2014-11-04",
            "2017-09-12", "2017-09-12", "2017-09-12", "2017-09-12",
            "2017-10-10", "2017-10-10", "2017-10-10", "2017-10-10",
            "2018-01-01", "2018-01-01", "2018-01-01", "2018-01-01",
            "2012-07-17", "2012-07-17",
            "2012-11-06", "2012-11-06",
            "2013-01-01", "2013-01-01",
            "2015-01-01", "2015-01-01", "2015-01-01",
            "2015-09-15", "2015-09-15", "2015-09-15",
            "2015-10-06", "2015-10-06", "2015-10-06",
            "2015-11-03", "2015-11-03", "2015-11-03",
            "2018-05-08", "2018-05-08", "2018-05-08", "2018-05-08",
            "2018-11-06", "2018-11-06", "2018-11-06", "2018-11-06",
            "2012-01-01",
            "2012-05-08",
            "2016-06-07", "2016-06-07", "2016-06-07",
            "2016-11-08", "2016-11-08", "2016-11-08",
            "2017-01-01", "2017-01-01", "2017-01-01", "2017-01-01",
            "2017-11-07", "2017-11-07", "2017-11-07", "2017-11-07"
    };
    private String[] attributes = {
            "house_num", "street_name", "zip_code", "phone_num", "age_group",
            "county_desc", "last_name", "res_city_desc", "ethnic_desc", "party_desc",
            "house_num", "street_name", "zip_code", "phone_num", "age_group",
            "house_num", "street_name", "zip_code", "phone_num", "age_group",
            "county_desc", "last_name", "res_city_desc", "ethnic_desc", "party_desc"
    };

    @Override
    public void configure(Configuration configuration) {
        this.configFileName = configuration.getString(CONFIG_FILE_NAME_OPTION);
        this.processingTime = configuration.getString(CONFIG_PROCESSING_TIME);
        this.outdatedFrequency = configuration.getString(CONFIG_OUTDATED_FREQUENCY);
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
        String[] ncidBatch = sheetInputSplit1.getNcid1().split("#");
        try {
            Class.forName("org.postgresql.Driver");
            Connection c = null;
            Statement stmt = null;
            c = DriverManager
                    .getConnection("jdbc:postgresql://localhost:5432/postgres",
                            "postgres", "postgres");
            if (ncidBatch.length > 0) {
                // Variables to store a value which detemrines the starting point in tghe attributes and dates array.
                int startAtAttribute = 0;
                int startAtDate = 0;

                // Iterate over all ncids in batch
                for (String ncid : ncidBatch) {
                    ReplacementEntry initialEntry = null;
                    stmt = c.createStatement();
                    String sql = "SELECT * FROM replacement WHERE nc_id='" + ncid + "' AND timestamp <= '2020-01-01' ORDER BY timestamp DESC LIMIT 1;";
                    ResultSet rs = stmt.executeQuery(sql);
                    while (rs.next()) {
                        initialEntry = new ReplacementEntry(
                                rs.getInt("id"),
                                rs.getString("nc_id"),
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
                                rs.getString("mail_state"),
                                rs.getString("mail_zipcode"));
                    }
                    if(initialEntry != null) {
                        for(int i = 0; i < Long.parseLong(this.outdatedFrequency); i++) {
                            String date = dates[startAtDate];
                            String attribute = attributes[startAtAttribute];
                            Field preField = ReplacementEntry.class.getField(attribute);
                            String preValue = (String) preField.get(initialEntry);

                            sql = "SELECT " + attribute +" FROM replacement WHERE nc_id='" + ncid + "' AND timestamp <= '"+ date +"' ORDER BY timestamp DESC LIMIT 1;";
                            rs = stmt.executeQuery(sql);
                            if(!rs.isBeforeFirst()) {
                                sql = "SELECT " + attribute +" FROM replacement WHERE nc_id='" + ncid + "' AND timestamp >= '"+ date +"' ORDER BY timestamp ASC LIMIT 1;";
                                rs = stmt.executeQuery(sql);
                            }

                            String newValue = null;
                            while(rs.next()) {
                                Field field = ReplacementEntry.class.getField(attribute);
                                field.set(initialEntry, rs.getString(1));
                                newValue = rs.getString(1);
                            }
                            if(preValue != null && !preValue.equals(newValue)){
                                System.out.println("#" + (i+1) + "---NCID: " + ncid + "---Date: " + date + "---Attribute: " + attribute + "---Old: " + preValue  + "---New: " + newValue);
                                //System.out.println("#" + (i+1) + "---SQL: " + sql);
                            }

                            if(startAtAttribute < attributes.length -1) {
                                startAtAttribute++;
                            } else {
                                startAtAttribute = 0;
                            }
                            if(startAtDate < dates.length -1) {
                                startAtDate++;
                            } else {
                                startAtDate = 0;
                            }
                        }
                    }

                    // Time-out the process to inject further errors in addition to outdated values. This should account
                    // for the fact that histories of different size take a different amount of time to process.
                    long timeout = (Integer.parseInt(this.outdatedFrequency) * Integer.parseInt(this.processingTime));
                    TimeUnit.MILLISECONDS.sleep(timeout);
                }
            }
            c.close();
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
