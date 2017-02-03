package csd.uoc.gr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public abstract class JobHelper {
    protected Job job;

    protected abstract String jobTitle();
    protected abstract String outputDirectoryNamed();

    public JobHelper(Configuration conf, Path inputPath) throws IOException {
        job = Job.getInstance(conf, this.jobTitle());

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(this.job, inputPath);
    }


    public Job getJob() {
        return job;
    }

    public Path execute(boolean verbose, Path outputDirectory) throws InterruptedException, IOException, ClassNotFoundException {
        Path finalResultPath = new Path(outputDirectory.toString() + "/" +  this.outputDirectoryNamed());
        FileOutputFormat.setOutputPath(this.job, finalResultPath);

        System.out.println("Starting job " + this.jobTitle());
        this.job.waitForCompletion(verbose);
        System.out.println("Job " + this.jobTitle() + " complete with output at " + finalResultPath);

        return finalResultPath;
    }
}
