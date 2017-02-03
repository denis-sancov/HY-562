package csd.uoc.gr.ThirdPhase;

import csd.uoc.gr.StringHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.NamedVector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class Main extends Configured implements Tool {

    /*
           args[0] - path to local fs directory with files
           args[1] - path for output result in hdfs
    */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Main(), args);
        System.exit(res);
    }


    public static ArrayList<String> retrieveTopicsUsingMalletLDA(Path documentsPath, Configuration conf, String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Performing Mallet LDA");

        System.out.println("Executing second phase...");
        csd.uoc.gr.SecondPhase.Main.PhaseResultObject phaseResultObject = csd.uoc.gr.SecondPhase.Main.executeSecondPhase(conf, args);

        List<NamedVector> clusteredDocs = csd.uoc.gr.SecondPhase.Main.readClustersFromOutput(phaseResultObject.getkMeansPath(), conf);

        MalletHelper malletWrapper = new MalletHelper(50);

        return malletWrapper.findTopicTerms(documentsPath, clusteredDocs);
    }


    public static ArrayList<String> retrieveTopicsUsingMahoutLDA(Path inputDirectory, Path outputDirectory, Configuration conf) throws IOException, InterruptedException {
        System.out.println("Performing Mahout LDA");

        StringBuilder sb = new StringBuilder();

        sb.append("#!/bin/bash\n");

        /*
               Directory to sequential files
        */
        String inputPath = inputDirectory.toString();
        String outputPath = outputDirectory.toString();


        sb.append("echo \"input is " + inputPath + " \" \n" +
                  "echo \"output is " + outputPath + " \" \n" +
                  "outSeqdir=\"" + outputPath + "/mahout/out-seqdir\" \n" +
                  "mahout seqdirectory -i " + inputPath +" " +
                  "-o $outSeqdir -c UTF-8 -chunk 5\n" +
                  "echo \"mahout seqdirectory is ready\"\n"
        );


        /*
              Sparse vectors from sequential files
        */
        sb.append("outSeqdirSparse=" + outputPath + "/mahout/out-seqdir-sparse\n" +
                  "mahout seq2sparse -i $outSeqdir " +
                  "-o $outSeqdirSparse --namedVector -wt tf -seq -nr 3\n" +
                  "echo \"mahout seq2sparse is ready\"\n"
        );


        /*
              TFIDF to matrix
        */
        sb.append("matrix=" + outputPath + "/mahout/matrix\n" +
                "mahout rowid -i \"$outSeqdirSparse/tf-vectors\" " +
                "-o $matrix\n" +
                "echo mahout matrix is ready\n"
        );

        /*
              LDA
        */
        sb.append("doOut=" + outputPath + "/mahout/cvb/do_out\n" +
                "toOut=" + outputPath + "/mahout/cvb/to_out\n" +
                "mahout cvb0_local -i \"$matrix/matrix\" " +
                "-d \"$outSeqdirSparse/dictionary.file-0\" " +
                "-a 0.5 -top 4 -do $doOut -to $toOut\n" +
                "echo mahout cvb0_local is ready\n"
        );

        /*
              Output
        */

        String userHome = System.getProperty("user.home");
        System.out.println("home is " + userHome);

        String mahoutLDAOut = userHome + "/mahout_output";
        sb.append("ldaOutput=\""+mahoutLDAOut+"\"\n" +
                "mahout vectordump -i $toOut " +
                "--dictionary \"$outSeqdirSparse/dictionary.file-0\" " +
                "--dictionaryType sequencefile --vectorSize 50 " +
                "-o $ldaOutput " +
                "-sort $toOut\n" +
                "echo mahout lda output is ready\n"
        );

        ProcessBuilder pb = new ProcessBuilder("/bin/bash");
        Process bash = pb.start();

        // Pass commands to the shell
        PrintStream ps = new PrintStream(bash.getOutputStream());
        ps.println(sb);
        ps.close();

        // Get an InputStream for the stdout of the shell
        BufferedReader br = new BufferedReader(
                new InputStreamReader(bash.getInputStream()));

        // Retrieve and print output
        String line;
        while (null != (line = br.readLine())) {
            System.out.println("> "+line);
        }
        br.close();


        Path mahoutLDAHDFSPath = new Path(outputPath + "/mahout/lda_output");

        FileSystem hdfs = FileSystem.get(conf);
        hdfs.moveFromLocalFile(new Path(mahoutLDAOut), mahoutLDAHDFSPath);

        br = new BufferedReader(new InputStreamReader(hdfs.open(mahoutLDAHDFSPath)));

        ArrayList<String> topics = new ArrayList<>();

        Set<String> hs = new HashSet<>();

        line = br.readLine();
        while (line != null) {
            line = line.replaceAll("[^a-zA-Z]", " ");
            line = StringHelper.removeStopWordsFromSentence(line);

            String[] result = line.split("\\s+");
            for (int i = 0; i < result.length; i++) {
                String tmp = result[i].trim();
                if (tmp.length() > 0) {
                    hs.add(tmp);
                }
            }
            line = br.readLine();
        }
        topics.addAll(hs);

        System.out.println("Mahout LDA done with topics " + topics.toString());

        return topics;
    }


    public static void countTopics(ArrayList<String> topics,  String topicProvider, Path outputPath, Path allDocs, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        String[] _topics = new String[topics.size()];
        int i = 0;
        for (String topic : topics) {
            _topics[i] = topic;
            i++;
        }

        FileSystem hdfs = FileSystem.get(conf);

        Path path = new Path(outputPath + "/topics-" + topicProvider);
        if (!hdfs.exists(path)) {
            hdfs.mkdirs(path);
        }

        Path cacheFile = new Path(path.toString() + "/cached_topics");
        System.out.println("cache = " + cacheFile + " result = " + path);

        FSDataOutputStream out = hdfs.create(cacheFile);
        WritableUtils.writeStringArray(out, _topics);
        out.close();

        TopicTermsCountJob topicTermsCountJob = new TopicTermsCountJob(conf, allDocs);
        topicTermsCountJob.getJob().addCacheFile(cacheFile.toUri());
        topicTermsCountJob.execute(true, path);

        System.out.println("cache = " + cacheFile + " result = " + path);
    }


    @Override
    public int run(String[] args) throws Exception {
        Path localInputPath = new Path(args[0]);
        Path hdfsSampleDataPath = new Path("/data");

        FileSystem hdfs = FileSystem.get(this.getConf());
        if (hdfs.exists(hdfsSampleDataPath)) {
            System.err.println("sample data directory is occupied with something, removing");
            hdfs.delete(hdfsSampleDataPath, true);
        }
        hdfs.copyFromLocalFile(localInputPath, hdfsSampleDataPath);

        Path outputPath = new Path(args[1]);
        if (hdfs.exists(outputPath)) {
            System.err.println("output path exists, removing");
            hdfs.delete(outputPath, true);
        }

        return performThirdPhase(args, getConf());
    }

    public static int performThirdPhase(String[] args, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException {

        Path localInputPath = new Path(args[0]);
        Path hdfsSampleDataPath = new Path("/data");

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(hdfsSampleDataPath)) {
            System.err.println("sample data directory is occupied with something, removing");
            hdfs.delete(hdfsSampleDataPath, true);
        }
        hdfs.copyFromLocalFile(localInputPath, hdfsSampleDataPath);

        Path outputPath = new Path(args[1]);
        if (hdfs.exists(outputPath)) {
            System.err.println("output path exists, removing");
            hdfs.delete(outputPath, true);
        }

        String[] argsForSecondPhase = new String[]{hdfsSampleDataPath.toString(), args[1]};

        ArrayList<String> malletTopics = retrieveTopicsUsingMalletLDA(localInputPath, conf, argsForSecondPhase);
        countTopics(malletTopics, "mallet" , outputPath, hdfsSampleDataPath, conf);

        ArrayList<String> mahoutTopics = retrieveTopicsUsingMahoutLDA(hdfsSampleDataPath, outputPath, conf);
        countTopics(mahoutTopics, "mahout", outputPath, hdfsSampleDataPath, conf);

        return 0;
    }
}
