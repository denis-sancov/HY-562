package csd.uoc.gr.SecondPhase;

import csd.uoc.gr.JobHelper;
import csd.uoc.gr.SentenceXMLInputFormat;
import csd.uoc.gr.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class WordFreqPerDocJob extends JobHelper {

    public WordFreqPerDocJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(SentenceXMLInputFormat.class);
    }

    @Override
    protected String jobTitle() {
        return "Word frequency in doc job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "word_freq_per_doc";
    }

    private static class JobMapper extends Mapper <LongWritable, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text resultKey = new Text();

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String file = ((FileSplit) context.getInputSplit()).getPath().getName();

            String line = StringHelper.stringByFilterString(value.toString());

            String tmp;
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                tmp = itr.nextToken();
                if (tmp.length() == 0) {
                    continue;
                }
                resultKey.set(tmp + "@" + file);
                context.write(resultKey, one);
            }
        }

    }


    private static class JobReducer extends Reducer <Text, IntWritable, Text, IntWritable> {

        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
