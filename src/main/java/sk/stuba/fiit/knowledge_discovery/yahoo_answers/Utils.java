package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

public final class Utils {

    private static final Stemmer stemmer = new Stemmer();

    private static String stopWords = "";

    static {
        try {
            StringBuilder sb = new StringBuilder();

            Charset charset = Charset.forName("UTF-8");

            for (String word : Files.readAllLines(Paths.get("stopwords.txt"), charset)) {
                word = word.trim();

                if (sb.length() != 0) {
                    sb.append("|");
                }

                sb.append(word);
            }

            stopWords = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Utils() {
    }

    private static String stemWord(final String word) {
        for (int i = 0; i < word.length(); i++) {
            stemmer.add(word.charAt(i));
        }

        stemmer.stem();

        return stemmer.toString();
    }

    private static String stemWords(final String words) {
        StringBuilder sb = new StringBuilder();

        String[] parts = words.split("\\s+");

        for (int i = 0; i < parts.length; i++) {
            parts[i] = stemWord(parts[i]);

            sb.append(parts[i]);

            if (i + 1 < parts.length) {
                sb.append(" ");
            }
        }

        return sb.toString();
    }

    public static String normalizeString(String s) {
        s = s.trim().toLowerCase();

        // Remove html tags
        s = s.replaceAll("<[a-z]+( ?/)?>", "");

        s = s.replaceAll("([a-z]+)(')([a-z]+)", "$1$3");

        // Remove non-word characters
        s = s.replaceAll("\\s+\\W\\s+", " ");

        // Remove stop words
        if (stopWords.length() > 0) {
            s = s.replaceAll("\\b(" + stopWords + ")\\b", "");
        }

        s = s.replaceAll("[\\s\\W_]+", " ");

        // Remove numbers
        s = s.replaceAll("\\s+\\d+(.\\d++)?\\s+", " ");

        // Remove short words
//        s = s.replaceAll("^[\\w]{1,1}\\s+|\\s+[\\w]{1,1}\\s+|\\s+[\\w]{1,1}$", " ");

        return stemWords(s.trim());
    }

}
