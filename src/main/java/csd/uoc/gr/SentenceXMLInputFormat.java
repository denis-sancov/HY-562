package csd.uoc.gr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Originally is made by mahoout
 */
public class SentenceXMLInputFormat extends TextInputFormat {

    public RecordReader<LongWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) {
        return new SentenceRecordReader();
    }

    public static class SentenceRecordReader extends RecordReader<LongWritable, Text> {
        private LongWritable key = new LongWritable();
        private Text value = new Text();

        private byte[] startTag;
        private byte[] endTag;

        private long start;
        private long end;
        private FSDataInputStream fsin;

        private DataOutputBuffer buffer = new DataOutputBuffer();


        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

            Configuration conf = taskAttemptContext.getConfiguration();
            startTag = "<sentence id=".getBytes("utf-8");
            endTag = "</sentence>".getBytes("utf-8");

            FileSplit fileSplit = (FileSplit)inputSplit;

            start = fileSplit.getStart();
            end = start + fileSplit.getLength();
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            fsin = fs.open(fileSplit.getPath());
            fsin.seek(start);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (fsin.getPos() < end) {
                if (readUntilMatch(startTag, false)) {
                    try {
                        buffer.write(startTag);
                        if (readUntilMatch(endTag, true)) {
                            key.set(fsin.getPos());
                            value.set(buffer.getData(), 0,
                                    buffer.getLength());
                            return true;
                        }
                    } finally {
                        buffer.reset();
                    }
                }
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (fsin.getPos() - start) / (float) (end - start);
        }

        @Override
        public void close() throws IOException {
            fsin.close();
        }



        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
            int i = 0;
            while (true) {
                int b = fsin.read();
                // end of file:
                if (b == -1)
                    return false;
                // save to buffer:
                if (withinBlock)
                    buffer.write(b);
                // check if we're matching:
                if (b == match[i]) {
                    i++;
                    if (i >= match.length)
                        return true;
                } else
                    i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && fsin.getPos() >= end)
                    return false;
            }
        }
    }
}
