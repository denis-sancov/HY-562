package csd.uoc.gr.SecondPhase;

import csd.uoc.gr.JobHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class TFIDFToVectorJob extends JobHelper {

    public TFIDFToVectorJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VectorWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
    }

    @Override
    protected String jobTitle() {
        return "TF-IDF to Vector Writable job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "tf_idf_vectors";
    }

    private static class JobMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        DoubleWritable outputValue = new DoubleWritable();

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] wordAndCounters = value.toString().split("\t");
            double tf_idfValue = Double.parseDouble(wordAndCounters[1]);
            String doc = wordAndCounters[0].split("@")[1];

            outputValue.set(tf_idfValue);
            context.write(new Text(doc), outputValue);
        }
    }


    private static class JobReducer extends Reducer<Text, DoubleWritable, Text, VectorWritable> {

        private VectorWritable vectorWritable = new VectorWritable();

        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            List<Double> items = new ArrayList<>();

            for (DoubleWritable value : values) {
                items.add(value.get());
            }

            DenseVector vector = new DenseVector(10000);
            double[] target = new double[items.size()];
            for (int i = 0; i < target.length; i++) {
                vector.set(i,Math.round(items.get(i)*100.0)/100.);
            }

            NamedVector nvector = new NamedVector(vector, key.toString());

            vectorWritable.set(nvector);
            context.write(key, vectorWritable);
        }

    }
}
