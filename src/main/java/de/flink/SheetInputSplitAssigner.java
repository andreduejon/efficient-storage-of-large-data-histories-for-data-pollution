package de.flink;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;

public class SheetInputSplitAssigner implements InputSplitAssigner {

    private List<SheetInputSplit1> remainingInputSplits;

    public SheetInputSplitAssigner(SheetInputSplit1[] inputSplits) {
        remainingInputSplits = Lists.newArrayList(inputSplits);
    }

    @Override
    public SheetInputSplit1 getNextInputSplit(String host, int taskId) {
        SheetInputSplit1[] remainingInputSplitsForHost = remainingInputSplits.stream()
                .filter(sheetInputSplit -> sheetInputSplit.getHostName().equals(host)).toArray(SheetInputSplit1[]::new);
        SheetInputSplit1 result = null;
        if (remainingInputSplitsForHost.length > 0) {
            result = remainingInputSplitsForHost[0];
            remainingInputSplits.remove(result);
            log("Assigned to host " + host + ", taskId " + taskId);
        } else {
            log("No split for host " + host + ", taskId " + taskId);
        }
        return result;
    }

    @Override
    public void returnInputSplit(List<InputSplit> list, int i) {
        log("Some InputSplits could not be read");
    }

    private void log(String s) {
        System.out.println("SheetInputSplitAssigner: " + s);
    }
}
