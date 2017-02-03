package csd.uoc.gr.ForthPhase;

import csd.uoc.gr.JobHelper;
import csd.uoc.gr.SentenceXMLInputFormat;
import csd.uoc.gr.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class SentenceFilteringJob extends JobHelper {

    public SentenceFilteringJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SentenceXMLInputFormat.class);
    }

    @Override
    protected String jobTitle() {
        return "Sentence filtering job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "filtered_document";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Text sentence = new Text();

        private List<String> topicTerms = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

            URI[] cacheFiles = context.getCacheFiles();

            if ((cacheFiles != null) && (cacheFiles.length > 0)) {
                FileSystem hdfs = FileSystem.get(context.getConfiguration());

                FSDataInputStream inputStream = hdfs.open(new Path(cacheFiles[0].toString()));
                String[] items = WritableUtils.readStringArray(inputStream);
                topicTerms = Arrays.asList(items);
                inputStream.close();
            }
        }

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String sentenceLine = StringHelper.stringByFilterString(value.toString());

            boolean canUseSentence = false;
            if (topicTerms != null) {
                StringTokenizer itr = new StringTokenizer(sentenceLine);
                while (itr.hasMoreTokens()) {
                    String nextToken = itr.nextToken();
                    if (topicTerms.contains(nextToken)) {
                        canUseSentence = true;
                        break;
                    }
                }
            } else {
                canUseSentence = true;
            }


            if (canUseSentence) {
                this.sentence.set(sentenceLine);
                context.write(this.sentence, NullWritable.get());
            }

        }
    }

    private static class JobReducer extends Reducer<Text, NullWritable, NullWritable, Text> {
        ArrayList<String> sentences = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            sentences.add(key.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            ArrayList<String> result = new ArrayList<>();


            boolean canBeUsed;
            for (int i = 0; i < sentences.size(); ++i) {
                String first = sentences.get(i);

                canBeUsed = true;

                for (int j = i + 1; j < sentences.size(); ++j) {
                    String second = sentences.get(j);
                    if (JaccardSimilarity.similarity(first, second) >= 0.8) {
                        canBeUsed = false;
                        break;
                    }
                }
                if (canBeUsed == true) {
                    result.add(sentences.get(i));
                }
            }

            for (String sentence : result) {
                context.write(NullWritable.get(), new Text(sentence));
            }
        }
    }
}
