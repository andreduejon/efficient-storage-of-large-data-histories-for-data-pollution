package de.flinkmath;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;

import java.io.*;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
            MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

            MongoDatabase database = mongoClient.getDatabase("history");
            MongoCollection<Document> collection = database.getCollection("data-replacement");

            if (ncidBatch.length > 0) {
                // Variables to store a value which detemrines the starting point in tghe attributes and dates array.
                int startAtAttribute = 0;
                int startAtDate = 0;

                // Iterate over all ncids in batch
                for (String ncid : ncidBatch) {
                    Document myDoc = collection.find(eq("_id", ncid)).first();
                    ReplacementEntry initialEntry = null;
                    List<Document> entries = (List<Document>) myDoc.get("data-history");

                    if(!entries.isEmpty()) {
                        Document mostRecent = entries.get(0);
                        initialEntry = new ReplacementEntry(
                                mostRecent.getInteger("id"),
                                ncid,
                                LocalDateTime.ofInstant(mostRecent.getDate("timestamp").toInstant(), ZoneId.systemDefault()),
                                mostRecent.getString("county_id"),
                                mostRecent.getString("county_desc"),
                                mostRecent.getString("last_name"),
                                mostRecent.getString("first_name"),
                                mostRecent.getString("midl_name"),
                                mostRecent.getString("house_num"),
                                mostRecent.getString("street_dir"),
                                mostRecent.getString("street_name"),
                                mostRecent.getString("res_city_desc"),
                                mostRecent.getString("state_cd"),
                                mostRecent.getString("zip_code"),
                                mostRecent.getString("area_cd"),
                                mostRecent.getString("phone_num"),
                                mostRecent.getString("race_code"),
                                mostRecent.getString("race_desc"),
                                mostRecent.getString("ethnic_code"),
                                mostRecent.getString("ethnic_desc"),
                                mostRecent.getString("party_cd"),
                                mostRecent.getString("party_desc"),
                                mostRecent.getString("sex_code"),
                                mostRecent.getString("sex"),
                                mostRecent.getString("age"),
                                mostRecent.getString("birth_place"),
                                mostRecent.getString("age_group"),
                                mostRecent.getString("name_prefx_cd"),
                                mostRecent.getString("name_sufx_cd"),
                                mostRecent.getString("half_code"),
                                mostRecent.getString("street_type_cd"),
                                mostRecent.getString("street_sufx_cd"),
                                mostRecent.getString("unit_designator"),
                                mostRecent.getString("unit_num"),
                                mostRecent.getString("mail_addr1"),
                                mostRecent.getString("mail_addr2"),
                                mostRecent.getString("mail_addr3"),
                                mostRecent.getString("mail_addr4"),
                                mostRecent.getString("mail_city"),
                                mostRecent.getString("mail_state"),
                                mostRecent.getString("mail_zipcode")
                        );
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
            mongoClient.close();
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
   this.sheetInputSplit1 = sheetInputSplit1;
           long startTime1 = System.nanoTime();
           String[] ncidBatch = sheetInputSplit1.getNcid1().split("#");
           long endTime1 = System.nanoTime();
           long timeElapsed = endTime1 - startTime1;
           try {
           long startTime2 = System.nanoTime();
           MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");

           MongoDatabase database = mongoClient.getDatabase("history");
           MongoCollection<Document> collection = database.getCollection("data");
        long endTime2 = System.nanoTime();
        long timeElapsed2 = endTime2 - startTime2;

        long startTime3 = System.nanoTime();
        if(ncidBatch.length > 0) {
        for (String ncid: ncidBatch) {
        Document myDoc = collection.find(eq("_id", ncid)).first();


        List<Document> checkpoints = (List<Document>) myDoc.get("checkpoints");
        Document initialCheckpointDoc = checkpoints.get(0);
        Checkpoint checkpoint = new Checkpoint(
        ncid,
        initialCheckpointDoc.getInteger("checkpoint_id"),
        null,
        LocalDateTime.ofInstant(initialCheckpointDoc.getDate("timestamp").toInstant(), ZoneId.systemDefault()),
        initialCheckpointDoc.getString("county_id"),
        initialCheckpointDoc.getString("county_desc"),
        initialCheckpointDoc.getString("last_name"),
        initialCheckpointDoc.getString("first_name"),
        initialCheckpointDoc.getString("midl_name"),
        initialCheckpointDoc.getString("house_num"),
        initialCheckpointDoc.getString("street_dir"),
        initialCheckpointDoc.getString("street_name"),
        initialCheckpointDoc.getString("res_city_desc"),
        initialCheckpointDoc.getString("state_cd"),
        initialCheckpointDoc.getString("zip_code"),
        initialCheckpointDoc.getString("area_cd"),
        initialCheckpointDoc.getString("phone_num"),
        initialCheckpointDoc.getString("race_code"),
        initialCheckpointDoc.getString("race_desc"),
        initialCheckpointDoc.getString("ethnic_code"),
        initialCheckpointDoc.getString("ethnic_desc"),
        initialCheckpointDoc.getString("party_cd"),
        initialCheckpointDoc.getString("party_desc"),
        initialCheckpointDoc.getString("sex_code"),
        initialCheckpointDoc.getString("sex"),
        initialCheckpointDoc.getString("age"),
        initialCheckpointDoc.getString("age_group"),
        initialCheckpointDoc.getString("name_prefx_cd"),
        initialCheckpointDoc.getString("name_sufx_cd"),
        initialCheckpointDoc.getString("half_code"),
        initialCheckpointDoc.getString("street_type_cd"),
        initialCheckpointDoc.getString("street_sufx_cd"),
        initialCheckpointDoc.getString("unit_designator"),
        initialCheckpointDoc.getString("unit_num"),
        initialCheckpointDoc.getString("mail_addr1"),
        initialCheckpointDoc.getString("mail_addr2"),
        initialCheckpointDoc.getString("mail_addr3"),
        initialCheckpointDoc.getString("mail_addr4"),
        initialCheckpointDoc.getString("mail_city"),
        initialCheckpointDoc.getString("mail_state"),
        initialCheckpointDoc.getString("mail_zipcode")
        );
        obj.addCheckPointList(checkpoint);

        List<Document> updates = (List<Document>) myDoc.get("updates");
        for(Document updateDoc: updates) {
        Update update = new Update(
        ncid,
        updateDoc.getInteger("update"),
        updateDoc.getInteger("update-group"),
        LocalDateTime.ofInstant(updateDoc.getDate("timestamp").toInstant(), ZoneId.systemDefault()),
        updateDoc.getString("attribute"),
        updateDoc.getString("value")
        );
        obj.addUpdateList(update);
        }

        // Time-out the process to simulate more realistic calculation pattern. This should account
        // for the fact that histories of different size take a different amount of time to process.
        long timeout = Math.round(obj.getUpdateList().size() * Float.parseFloat(this.processingTime));
        TimeUnit.MILLISECONDS.sleep(updates.size());
        }
        }
        long endTime3 = System.nanoTime();
        long timeElapsed3 = endTime3 - startTime3;
        mongoClient.close();
        System.out.println("Finished processing Batch. Stats(Prepare Batch: " + timeElapsed / 1000000 + "ms, Processed batch in: " + timeElapsed3 / 1000000 + "ms, DB connection time: " + timeElapsed2 / 1000000 + "ms.)");
        } catch (Exception e) {
        e.printStackTrace();
        System.err.println(e.getClass().getName()+": "+e.getMessage());
        System.exit(0);
        }