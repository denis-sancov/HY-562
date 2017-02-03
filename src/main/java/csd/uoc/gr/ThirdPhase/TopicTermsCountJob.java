package csd.uoc.gr.ThirdPhase;

import csd.uoc.gr.JobHelper;
import csd.uoc.gr.SentenceXMLInputFormat;
import csd.uoc.gr.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class TopicTermsCountJob extends JobHelper {

    public TopicTermsCountJob(Configuration conf, Path inputPath) throws IOException {
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
        return "Topic terms Count Job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "topic_terms_count";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private List<String> topicTerms;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                FileSystem hdfs = FileSystem.get(context.getConfiguration());
                Path path = new Path(cacheFiles[0].toString());

                FSDataInputStream inputStream = hdfs.open(path);
                String[] items = WritableUtils.readStringArray(inputStream);
                topicTerms = Arrays.asList(items);
                inputStream.close();;
            }
        }

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = StringHelper.stringByFilterString(value.toString());
            if (line == null) {
                return;
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String nextToken = itr.nextToken();
                if (!topicTerms.contains(nextToken)) {
                    continue;
                }
                word.set(nextToken);
                context.write(word, one);
            }
        }
    }

    private static class JobReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
