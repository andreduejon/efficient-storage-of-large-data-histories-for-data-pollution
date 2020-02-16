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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
                    HistoryObjects obj = new HistoryObjects();

                    obj.setNcid(ncid);
                    obj.setUpdates(myDoc.getInteger("update-count"));
                    obj.setCheckpoints(myDoc.getInteger("checkpoint-count"));
                    obj.setUpdateGroups(myDoc.getInteger("update-group-count"));
                    obj.setCheckpointList(new ArrayList<>());
                    obj.setUpdateList(new ArrayList<>());

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
                }
            }
            long endTime3 = System.nanoTime();
            long timeElapsed3 = endTime3 - startTime3;
            System.out.println("Finished processing Batch. Stats(Prepare Batch: " + timeElapsed / 1000000 + "ms, Processed batch in: " + timeElapsed3 / 1000000 + "ms, DB connection time: " + timeElapsed2 / 1000000 + "ms.)");
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
