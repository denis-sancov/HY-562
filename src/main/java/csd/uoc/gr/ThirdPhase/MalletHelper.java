package csd.uoc.gr.ThirdPhase;

import cc.mallet.pipe.*;
import cc.mallet.pipe.iterator.FileIterator;
import cc.mallet.topics.ParallelTopicModel;
import cc.mallet.types.Alphabet;
import cc.mallet.types.IDSorter;
import cc.mallet.types.InstanceList;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.NamedVector;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class MalletHelper {
    public int numberOfTopics = 100;

    private Pipe pipe;

    MalletHelper(int numberOfTopics) {
        this.numberOfTopics = numberOfTopics;
        ArrayList pipeList = new ArrayList();
        // Read data from File objects
        pipeList.add(new Input2CharSequence("UTF-8"));


        Pattern pattern = Pattern.compile("<catchphrase [^>]*>(.*?)</catchphrase>|<name>(.*?)</name>|<AustLII>(.*?)</AustLII>|<[^>]+>");
        pipeList.add(new CharSequenceReplace(pattern, ""));

        Pattern tokenPattern =
                Pattern.compile("[\\p{L}_]+");
        pipeList.add(new CharSequence2TokenSequence(tokenPattern));


        pipeList.add(new TokenSequenceLowercase());

        pipeList.add(new TokenSequenceRemoveStopwords(false, false));

        pipeList.add(new TokenSequence2FeatureSequence());

        pipe = new SerialPipes(pipeList);
    }

    public ArrayList<String> findTopicTerms(Path documentPath, List<NamedVector> clusters) throws IOException {
        System.out.println("getting terms from path " + documentPath.toString());
        FileIterator fileIterator = new FileIterator(
                new File[]{
                        new File(documentPath.toString())
                },
                new TxtFilter(clusters),
                FileIterator.LAST_DIRECTORY
        );
        // Construct a new instance list, passing it the pipe
        //  we want to use to process instances.
        InstanceList instances = new InstanceList(pipe);

        // Now process each instance provided by the iterator.
        instances.addThruPipe(fileIterator);


        ParallelTopicModel lda = new ParallelTopicModel(numberOfTopics);
        lda.setRandomSeed(5);
        lda.addInstances(instances);
        lda.setNumThreads(2);

        // Run the model for 50 iterations and stop (this is for testing only,
        //  for real applications, use 1000 to 2000 iterations)
        lda.setNumIterations(1000);
        lda.estimate();

        ArrayList<String> topicTerms = new ArrayList<>();

        Alphabet dataAlphabet = instances.getDataAlphabet();
        ArrayList<TreeSet<IDSorter>> topicSortedWords = lda.getSortedWords();

        for (int topic = 0; topic < this.numberOfTopics; topic++) {
            Iterator<IDSorter> iterator = topicSortedWords.get(topic).iterator();

            int rank = 0;
            while (iterator.hasNext() && rank < 15) {
                IDSorter idCountPair = iterator.next();
                String term = (String) dataAlphabet.lookupObject(idCountPair.getID());
                topicTerms.add(term);
                rank++;
            }
        }
        return topicTerms;
    }

    class TxtFilter implements FileFilter {
        private ArrayList<String> clusterNames = new ArrayList<>();

        TxtFilter(List<NamedVector> clusters) {
            for (NamedVector vector : clusters) {
                clusterNames.add(vector.getName());
            }
        }
        public boolean accept(File file) {
            System.out.println("check name " + file.getName());
            return clusterNames.contains(file.getName());
        }
    }
}
