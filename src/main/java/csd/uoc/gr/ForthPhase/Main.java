package csd.uoc.gr.ForthPhase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import static csd.uoc.gr.ThirdPhase.Main.performThirdPhase;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class Main extends Configured implements Tool {


    /*
           args[0] - path to local fs directory with files
           args[1] - path where to output results in hdfs
    */

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }



    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = this.getConf();

        String thirdPhaseOutput = "/project_out";
        String[] thirdPhaseArgs = {
                args[0],
                thirdPhaseOutput
        };
        performThirdPhase(thirdPhaseArgs, configuration);

        Path hdfsSampleDataPath = new Path("/data");
        Path outputPath = new Path(args[1]);
        Path availableTFIDFS = new Path(thirdPhaseOutput + "/tf_idf_values/part-r-00000");

        SentenceRankingJob rankingJob = new SentenceRankingJob(configuration, hdfsSampleDataPath);
        rankingJob.getJob().addCacheFile(availableTFIDFS.toUri());
        Path topSentencesPath = rankingJob.execute(true, outputPath);


        SentenceFilteringJob topRankFilterJob = new SentenceFilteringJob(configuration, topSentencesPath);
        topRankFilterJob.getJob().setInputFormatClass(TextInputFormat.class);
        topRankFilterJob.execute(true, new Path(outputPath.toString() + "/top_rank_doc"));



        SentenceFilteringJob malletFilterJob = new SentenceFilteringJob(configuration, hdfsSampleDataPath);
        malletFilterJob.getJob().addCacheFile(new Path(thirdPhaseOutput + "/topics-mallet/cached_topics").toUri());
        malletFilterJob.execute(true, new Path(outputPath.toString() + "/mallet_doc"));




        SentenceFilteringJob mahoutFilterJob = new SentenceFilteringJob(configuration, hdfsSampleDataPath);
        mahoutFilterJob.getJob().addCacheFile(new Path(thirdPhaseOutput + "/topics-mahout/cached_topics").toUri());
        mahoutFilterJob.execute(true, new Path(outputPath.toString() + "/mahout_doc"));

        return 0;
    }

}
