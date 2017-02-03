package csd.uoc.gr.SecondPhase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.common.iterator.sequencefile.PathFilters;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirValueIterable;
import org.apache.mahout.math.NamedVector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class Main extends Configured implements Tool {
    public final static int k = 5;

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        Main.executeSecondPhase(conf, args);
        return 0;
    }

    public static class PhaseResultObject {
        private Path kMeansPath = null;
        private Path clustersPath = null;

        PhaseResultObject(Path kMeansPath, Path clustersPath) {
            this.kMeansPath = kMeansPath;
            this.clustersPath = clustersPath;
        }

        public Path getkMeansPath() {
            return kMeansPath;
        }

        public Path getClustersPath() {
            return clustersPath;
        }
    }


    public static PhaseResultObject executeSecondPhase(Configuration conf, String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        FileSystem hdfs = FileSystem.get(conf);

        Path inputPath = new Path(args[0]);
        Path outputDirectory = new Path(args[1]);
        if (hdfs.exists(outputDirectory)) {
            hdfs.delete(outputDirectory, true);
        }

        FileStatus[] stat = hdfs.listStatus(inputPath);

        System.out.println("Documents count = " + stat.length);
        conf.set("DocumentsCount", String.valueOf(stat.length));


        /*
             First Job - Word freq in each doc
         */
        WordFreqPerDocJob job = new  WordFreqPerDocJob(conf, inputPath);
        Path wordFreqInDocResult = job.execute(true, outputDirectory);


        /*
             Second Job - Word Count for all docs
         */
        WordCountForDocsJob wordCountsForDocsJob = new WordCountForDocsJob(conf, wordFreqInDocResult);
        Path wordCountsForDocsResult = wordCountsForDocsJob.execute(true, outputDirectory);

        /*
            TF - IDF
         */

        TFIDFJob tfidfJob = new TFIDFJob(conf, wordCountsForDocsResult);
        Path tfIDFResultPath = tfidfJob.execute(true, outputDirectory);

        /*
            TF-IDF to vector writable
         */

        TFIDFToVectorJob tfidfToVectorJob = new TFIDFToVectorJob(conf, tfIDFResultPath);
        Path tfIDFVectorPath = tfidfToVectorJob.execute(true, outputDirectory);


        /*
            RandomSeedGenerator Work
         */
        Path tfIDFVectorResultPath = new Path(tfIDFVectorPath.toString() + "/part-r-00000");
        Path randomSeedOutput = new Path(outputDirectory + "/random_seed-output/");

        RandomSeedGenerator.buildRandom(conf, tfIDFVectorPath, randomSeedOutput, k, new ManhattanDistanceMeasure());

        System.out.println("canopy path = " + randomSeedOutput.toString());


        /*
             K-means
         */
        Path kMeansOutput = new Path(outputDirectory + "/k-means");

        KMeansDriver.run(tfIDFVectorResultPath,
                new Path(randomSeedOutput.toString() + "/part-randomSeed"),
                kMeansOutput,
                0.001,
                100,
                true,
                0,
                false
        );
        System.out.println("k-means path = " + kMeansOutput.toString());
        return new PhaseResultObject(kMeansOutput, randomSeedOutput);
    }

    public static List<NamedVector> readClustersWritable(Path clustersIn, Configuration conf) {
        List<NamedVector> clusters = new ArrayList<>();

        for (ClusterWritable value : new SequenceFileDirValueIterable<ClusterWritable>(clustersIn, PathType.LIST, PathFilters.logsCRCFilter(), conf)) {
            Cluster cluster = value.getValue();
            if (cluster.getCenter().getClass() == NamedVector.class) {
                NamedVector vector = (NamedVector) cluster.getCenter();
                clusters.add(vector);
            }
        }
        return clusters;
    }


    public static List<NamedVector> readClustersFromOutput(Path output, Configuration conf) throws IOException {
        List<NamedVector> clusters = new ArrayList<>();

        FileSystem fs = FileSystem.get(output.toUri(), conf);
        for (FileStatus s : fs.listStatus(output, new ClustersFilter())) {
            clusters.addAll(readClustersWritable(s.getPath(), conf));
        }
        return clusters;
    }

    static class ClustersFilter implements PathFilter {

        @Override
        public boolean accept(Path path) {
            String pathString = path.toString();
            return pathString.contains("/clusters-");
        }

    }

}
