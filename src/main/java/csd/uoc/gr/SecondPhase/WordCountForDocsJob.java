package csd.uoc.gr.SecondPhase;

import csd.uoc.gr.JobHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class WordCountForDocsJob extends JobHelper {

    public WordCountForDocsJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
    }

    @Override
    protected String jobTitle() {
        return "Word Counts for docs job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "word_counts_for_docs";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] wordAndDocCounter = value.toString().split("\t");
            String[] wordAndDoc = wordAndDocCounter[0].split("@");
            context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
        }
    }


    private static class JobReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int sumOfWordsInDocument = 0;
            Map<String, Integer> tempCounter = new HashMap<>();
            for (Text val : values) {
                String[] wordCounter = val.toString().split("=");
                tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
                sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
            }
            for (String wordKey : tempCounter.keySet()) {
                context.write(
                        new Text(wordKey + "@" + key.toString()),
                        new Text(tempCounter.get(wordKey) + "/" + sumOfWordsInDocument)
                );
            }
        }
    }

}
