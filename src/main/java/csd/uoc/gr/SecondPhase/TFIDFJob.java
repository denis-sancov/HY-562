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
public class TFIDFJob extends JobHelper {

    public TFIDFJob(Configuration conf, Path inputPath) throws IOException {
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
        return "Compute TF-IDF";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "tf_idf_values";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            String[] wordAndDoc = wordAndCounters[0].split("@");
            context.write(new Text(wordAndDoc[0]), new Text(wordAndDoc[1] + "=" + wordAndCounters[1]));
        }
    }

    private static class JobReducer extends Reducer <Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int numberOfDocumentsInCorpus = Integer.parseInt(context.getConfiguration().get("DocumentsCount"));

            int numberOfDocumentsInCorpusWhereKeyAppears = 0;

            Map<String, String> tempFrequencies = new HashMap<>();
            for (Text val : values) {
                String[] documentAndFrequencies = val.toString().split("=");
                numberOfDocumentsInCorpusWhereKeyAppears++;
                tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
            }
            for (String document : tempFrequencies.keySet()) {
                String[] wordFrequenceAndTotalWords = tempFrequencies.get(document).split("/");

                double tf = Double.valueOf(wordFrequenceAndTotalWords[0])
                        / Double.valueOf(wordFrequenceAndTotalWords[1]);

                double idf = (double) numberOfDocumentsInCorpus / (double) numberOfDocumentsInCorpusWhereKeyAppears;

                double tfIdf = numberOfDocumentsInCorpus == numberOfDocumentsInCorpusWhereKeyAppears ?
                        tf : tf * Math.log10(idf);

                context.write(new Text(key + "@" + document), new Text(String.valueOf(tfIdf)));
            }
        }
    }

}
