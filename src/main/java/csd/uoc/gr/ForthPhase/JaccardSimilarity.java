package csd.uoc.gr.ForthPhase;

import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by denis.sancov on 03/12/2016.
 */
public class JaccardSimilarity  {

    private static final Pattern SPACE_REG = Pattern.compile("\\s+");
    private static int k = 3;


    public static Map<String, Integer> getProfile(final String string) {
        HashMap<String, Integer> shingles = new HashMap<String, Integer>();

        String string_no_space = SPACE_REG.matcher(string).replaceAll(" ");
        for (int i = 0; i < (string_no_space.length() - k + 1); i++) {
            String shingle = string_no_space.substring(i, i + k);
            Integer old = shingles.get(shingle);
            if (old!=null) {
                shingles.put(shingle, old + 1);
            } else {
                shingles.put(shingle, 1);
            }
        }

        return Collections.unmodifiableMap(shingles);
    }


    public static double similarity(final String s1, final String s2) {
        Map<String, Integer> profile1 = getProfile(s1);
        Map<String, Integer> profile2 = getProfile(s2);

        Set<String> union = new HashSet<String>();
        union.addAll(profile1.keySet());
        union.addAll(profile2.keySet());

        int inter = 0;

        for (String key : union) {
            if (profile1.containsKey(key) && profile2.containsKey(key)) {
                inter++;
            }
        }

        return 1.0 * inter / union.size();
    }
}
