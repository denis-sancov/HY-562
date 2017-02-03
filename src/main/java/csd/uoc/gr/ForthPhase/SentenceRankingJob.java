package csd.uoc.gr.ForthPhase;

import csd.uoc.gr.JobHelper;
import csd.uoc.gr.SentenceXMLInputFormat;
import csd.uoc.gr.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Created by denis.sancov on 04/12/2016.
 */
public class SentenceRankingJob extends JobHelper {

    public final static int k = 400;

    public SentenceRankingJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setSortComparatorClass(DescendingKeyComparator.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(SentenceXMLInputFormat.class);
    }

    @Override
    protected String jobTitle() {
        return "Sentence rank job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "top_sentences_in_document";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Map<String, Float> tfIDFSValues = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            URI[] cacheFiles = context.getCacheFiles();

            if ((cacheFiles != null) && (cacheFiles.length > 0)) {

                String file = ((FileSplit) context.getInputSplit()).getPath().getName();

                FileSystem hdfs = FileSystem.get(context.getConfiguration());

                BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(new Path(cacheFiles[0].toString()))));

                String line;
                line = br.readLine();

                String splitResult[];
                String wordInfo[];
                String word;
                String document;

                while (line != null){
                    splitResult = line.split("\t");

                    wordInfo = splitResult[0].split("@");
                    document = wordInfo[1];
                    if (file.equals(document)) {
                        word = wordInfo[0].replaceAll("[^A-Za-z]","");
                        if (word.length() > 0) {
                            tfIDFSValues.put(word, Float.parseFloat(splitResult[1]));
                        }
                    }
                    line = br.readLine();
                }
                br.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String sentenceLine = StringHelper.stringByFilterString(value.toString());

            float sentenceTFIDF = 0.f;

            int components = 0;
            StringTokenizer itr = new StringTokenizer(sentenceLine);
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                if (tfIDFSValues.containsKey(nextToken)) {
                    sentenceTFIDF += tfIDFSValues.get(nextToken);
                }
                components += 1;
            }
            sentenceTFIDF = (1.f / components) * sentenceTFIDF;

            String result = sentenceTFIDF + "@" + sentenceLine;

            context.write(new Text(result), NullWritable.get());
        }
    }

    private static class JobReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        public int count = 0;
        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            if (count < k) {
                context.write(key, NullWritable.get());
                count += 1;
            }
        }
    }

}
