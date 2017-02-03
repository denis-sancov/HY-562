package csd.uoc.gr.FirstPhase;

import csd.uoc.gr.JobHelper;
import csd.uoc.gr.SentenceXMLInputFormat;
import csd.uoc.gr.StringHelper;
import net.didion.jwnl.JWNL;
import net.didion.jwnl.JWNLException;
import net.didion.jwnl.data.IndexWord;
import net.didion.jwnl.data.POS;
import net.didion.jwnl.data.Synset;
import net.didion.jwnl.data.Word;
import net.didion.jwnl.dictionary.Dictionary;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by denis.sancov on 02/12/2016.
 */
public class WordCountJob extends JobHelper {

    public WordCountJob(Configuration conf, Path inputPath) throws IOException {
        super(conf, inputPath);

        job.setMapperClass(JobMapper.class);
        job.setReducerClass(JobReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(SentenceXMLInputFormat.class);
    }

    @Override
    protected String jobTitle() {
        return "Word Count Job";
    }

    @Override
    protected String outputDirectoryNamed() {
        return "word_count";
    }


    private static class JobMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String line = StringHelper.stringByFilterString(value.toString());
            if (line == null) {
                return;
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private static class JobReducer extends Reducer<Text, IntWritable, Text, Text> {

        private Map<Text, IntWritable> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            countMap.put(new Text(key), new IntWritable(sum));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, IntWritable> sortedMap = sortByValues(countMap);


            InputStream fis = Main.class.getResourceAsStream("/config/file_properties.xml");
            try {
                JWNL.initialize(fis);
            } catch (JWNLException e) {
                e.printStackTrace();
            }

            Dictionary dictionary = Dictionary.getInstance();

            Text tmp = new Text();

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 15) {
                    break;
                }
                IndexWord word = getIndexWord(dictionary, key.toString());
                if (word == null) {
                    System.out.println("Nothing found for key : " + key);
                    counter--;
                    continue;
                }
                Synset[] syns = new Synset[0];
                try {
                    syns = word.getSenses();
                } catch (JWNLException e) {
                    e.printStackTrace();
                }


                StringBuilder result = new StringBuilder();
                result.append(sortedMap.get(key).toString());
                result.append(". Available syns are: ");
                for (Synset syn : syns) {
                    for (Word synWord : syn.getWords()) {

                        result.append(synWord.getLemma() + ", ");
                    }
                }
                tmp.set(result.toString());
                context.write(key, tmp);
            }
        }

        private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                @Override
                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }

        private static IndexWord getIndexWord(Dictionary dic, String key)  {
            IndexWord word = null;
            try {
                word = dic.getIndexWord(POS.NOUN, key);
                if (word != null) {
                    return word;
                }

                word = dic.getIndexWord(POS.ADJECTIVE, key);
                if (word != null) {
                    return word;
                }

                word = dic.getIndexWord(POS.VERB, key);
                if (word != null) {
                    return word;
                }

                word = dic.getIndexWord(POS.ADVERB, key);
                if (word != null) {
                    return word;
                }

            }
            catch (JWNLException e) {
                System.out.println(e.toString());
            }
            return word;
        }
    }

}
