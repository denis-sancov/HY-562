package csd.uoc.gr.FirstPhase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by denis.sancov on 01/12/2016.
 */
public class Main extends Configured implements Tool  {


    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = getConf();
        FileSystem hdfs = FileSystem.get(configuration);

        Path inputPath = new Path(args[0]);
        Path outputDirectory = new Path(args[1]);
        if (hdfs.exists(outputDirectory)) {
            hdfs.delete(outputDirectory, true);
        }

        /*
             First Job - Word freq in each doc
         */
        WordCountJob job = new WordCountJob(configuration, inputPath);
        job.execute(true, outputDirectory);

        return 0;
    }
}
