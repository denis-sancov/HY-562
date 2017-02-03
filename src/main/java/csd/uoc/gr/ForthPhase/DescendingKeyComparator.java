package csd.uoc.gr.ForthPhase;

import org.apache.hadoop.io.*;

/**
 * Created by denis.sancov on 05/12/2016.
 */
public class DescendingKeyComparator extends WritableComparator {
    protected DescendingKeyComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        Text key1 = (Text) w1;
        Text key2 = (Text) w2;
        return -1 * key1.compareTo(key2);
    }
}
