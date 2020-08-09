package de.flink;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.lang.reflect.Field;
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
            "20081104",
            "20090101",
            "20140101", "20140101",
            "20140506", "20140506",
            "20160101", "20160101", "20160101",
            "20160315", "20160315", "20160315",
            "20190101", "20190101", "20190101", "20190101",
            "20110101",
            "20100101",
            "20140715", "20140715",
            "20141104", "20141104",
            "20170912", "20170912", "20170912", "20170912",
            "20171010", "20171010", "20171010", "20171010",
            "20180101", "20180101", "20180101", "20180101",
            "20120717", "20120717",
            "20121106", "20121106",
            "20130101", "20130101",
            "20150101", "20150101", "20150101",
            "20150915", "20150915", "20150915",
            "20151006", "20151006", "20151006",
            "20151103", "20151103", "20151103",
            "20180508", "20180508", "20180508", "20180508",
            "20181106", "20181106", "20181106", "20181106",
            "20120101",
            "20120508",
            "20160607", "20160607", "20160607",
            "20161108", "20161108", "20161108",
            "20170101", "20170101", "20170101", "20170101",
            "20171107", "20171107", "20171107", "20171107"
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
    public BaseStatistics getStatistics(BaseStatistics baseStatistics) {
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
    public void open(SheetInputSplit1 sheetInputSplit1) {
        this.sheetInputSplit1 = sheetInputSplit1;
        String[] ncidBatch = sheetInputSplit1.getNcid1().split("#");
        try {
            org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

            try {
                org.apache.hadoop.hbase.client.Connection connection = ConnectionFactory.createConnection(config);
                Table table = connection.getTable(TableName.valueOf("history"));

                if (ncidBatch.length > 0) {
                    // Variables to store a value which detemrines the starting point in tghe attributes and dates array.
                    int startAtAttribute = 0;
                    int startAtDate = 0;

                    // Iterate over all ncids in batch
                    for (String ncid : ncidBatch) {
                        ReplacementEntry initialEntry = null;

                        Get get = new Get(Bytes.toBytes(ncid));
                        get.addFamily(Bytes.toBytes("data"));

                        Result rs = table.get(get);
                        if (rs.getExists()) {
                            initialEntry = new ReplacementEntry(
                                    0,
                                    ncid,
                                    null,
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("county_id"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("county_desc"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("last_name"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("first_name"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("midl_name"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("house_num"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("street_dir"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("street_name"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("res_city_desc"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("state_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("zip_code"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("area_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("phone_num"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("race_code"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("race_desc"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("ethnic_code"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("ethnic_desc"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("party_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("party_desc"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("sex_code"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("sex"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("age"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("birth_place"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("age_group"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("name_prefx_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("name_sufx_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("half_code"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("street_type_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("street_sufx_cd"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("unit_designator"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("unit_num"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_addr1"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_addr2"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_addr3"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_addr4"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_city"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_state"))),
                                    Bytes.toString(rs.getValue(Bytes.toBytes("data"),Bytes.toBytes("mail_zipcode"))));

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

                            // Time-out the process to inject further errors in addition to outdated values. This should account
                            // for the fact that histories of different size take a different amount of time to process.
                            long timeout = (Integer.parseInt(this.outdatedFrequency) * Integer.parseInt(this.processingTime));
                            TimeUnit.MILLISECONDS.sleep(timeout);
                        }
                    }
                    table.close();
                    connection.close();




            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
    }

    @Override
    public boolean reachedEnd() {
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
    public void close() {
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
