package de.flinkmath;

import com.mongodb.client.*;
import com.mongodb.client.model.UnwindOptions;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.bson.Document;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Aggregates.limit;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Aggregates.replaceRoot;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Aggregates.unwind;
import static com.mongodb.client.model.Projections.include;
import static com.mongodb.client.model.Sorts.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.concurrent.TimeUnit;

import java.io.*;
import java.lang.reflect.Field;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
                // Variables to store a value which determines the starting point in the attributes and dates array.
                int startAtAttribute = 0;
                int startAtDate = 0;

                // Iterate over all ncids in batch
                for (String ncid : ncidBatch) {
                    Document myDoc = collection.find(eq("_id", ncid)).first();
                    ReplacementEntry initialEntry = null;
                    List<Document> entries = null;
                    if (myDoc != null) {
                        entries = (List<org.bson.Document>) myDoc.get("data-history");
                    }

                    if (entries != null && !entries.isEmpty()) {
                        Document mostRecent = entries.get(entries.size() - 1);
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

                    if (initialEntry != null) {
                        for (int i = 0; i < Long.parseLong(this.outdatedFrequency); i++) {
                            String date = dates[startAtDate];
                            String attribute = attributes[startAtAttribute];
                            Field preField = ReplacementEntry.class.getField(attribute);
                            String preValue = (String) preField.get(initialEntry);

                            AggregateIterable<Document> findIterable = collection.aggregate(
                                    Arrays.asList(
                                            match(eq("_id", ncid)),
                                            unwind("$data-history", new UnwindOptions().preserveNullAndEmptyArrays(true)),
                                            replaceRoot("$data-history"),
                                            match(gte("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(date + "T00:00:00.000Z"))),
                                            sort(ascending("timestamp")),
                                            limit(1),
                                            project(include(attribute))));
                            if (!findIterable.iterator().hasNext()) {
                                findIterable =collection.aggregate(
                                        Arrays.asList(
                                                match(eq("_id", ncid)),
                                                unwind("$data-history", new UnwindOptions().preserveNullAndEmptyArrays(true)),
                                                replaceRoot("$data-history"),
                                                match(lte("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(date + "T00:00:00.000Z"))),
                                                sort(descending("timestamp")),
                                                limit(1),
                                                project(include(attribute))));
                            }
                            String newValue = null;

                            for (Document document : findIterable) {
                                Field field = ReplacementEntry.class.getField(attribute);
                                newValue = document.getString(attribute);
                                field.set(initialEntry, document.getString(attribute));
                            }

                            if (preValue != null && !preValue.equals(newValue)) {
                                System.out.println("#" + (i + 1) + "---NCID: " + ncid + "---Date: " + date + "---Attribute: " + attribute + "---Old: " + preValue + "---New: " + newValue);
                            }

                            if (startAtAttribute < attributes.length - 1) {
                                startAtAttribute++;
                            } else {
                                startAtAttribute = 0;
                            }
                            if (startAtDate < dates.length - 1) {
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
            }
            mongoClient.close();
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
    public HistoryObjects nextRecord(HistoryObjects toReuse) {
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