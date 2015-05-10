package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import com.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// http://technobium.com/tfidf-explained-using-apache-mahout/
public final class TfIdf {

    private final String outputFolder;

    private final Configuration configuration;

    private final FileSystem fileSystem;

    private final Path documentsSequencePath;

    private final Path tokenizedDocumentsPath;

    private final Path tfIdfPath;

    private final Path termFrequencyVectorsPath;

//    private final Map<String, Integer> wordCountMap;
//
//    private final Map<String, Integer> wordDictionaryMap;

    private Map<String, Question> uriToQuestion;

    public TfIdf() throws IOException {
//        wordCountMap = new HashMap<String, Integer>();
//        wordDictionaryMap = new HashMap<String, Integer>();

        configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);

        outputFolder = "output/";
        documentsSequencePath = new Path(outputFolder, "sequence");
        tokenizedDocumentsPath = new Path(outputFolder,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        tfIdfPath = new Path(outputFolder + "tfidf");
        termFrequencyVectorsPath = new Path(outputFolder +
                DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
    }

    public void putQuestions(final List<Question> questions) throws IOException {
        final SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem,
                configuration, documentsSequencePath, Text.class, Text.class);

        uriToQuestion = new HashMap<String, Question>();

        for (Question question : questions) {
            Text uri = new Text(question.getUri());
            Text text = new Text(question.toText());

            writer.append(uri, text);

            uriToQuestion.put(question.getUri(), question);
        }

        writer.close();
    }

    public void calculateTfIdf()
            throws ClassNotFoundException, IOException, InterruptedException {

        DocumentProcessor.tokenizeDocuments(documentsSequencePath,
                PorterAnalyzer.class, tokenizedDocumentsPath, configuration);

        DictionaryVectorizer.createTermFrequencyVectors(tokenizedDocumentsPath,
                new Path(outputFolder),
                DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
                configuration, 1, 1, 0.0f, PartialVectorMerger.NO_NORMALIZING,
                true, 1, 100, false, false);

        Pair<Long[], List<Path>> documentFrequencies = TFIDFConverter.
                calculateDF(termFrequencyVectorsPath, tfIdfPath,
                        configuration, 100);

        TFIDFConverter.processTfIdf(termFrequencyVectorsPath, tfIdfPath,
            configuration, documentFrequencies, 1, 100,
            PartialVectorMerger.NO_NORMALIZING, false, false, false, 1);

//        fillWordCountMap();
//        fillWordDictionaryMap();
    }

    private void printSequenceFile(final Path path) {
        final SequenceFileIterable<Writable, Writable> iterable =
                new SequenceFileIterable<Writable, Writable>(path, configuration);

        for (Pair<Writable, Writable> pair : iterable) {
            System.out.format("%15s > %s\n", pair.getFirst(), pair.getSecond());
        }
    }

    public void printAll() {
//        this.printSequenceFile(this.documentsSequencePath);

        System.out.println("\n Step 1: Word count ");
        this.printSequenceFile(getWordCountPath());

        System.out.println("\n Step 2: Word dictionary ");
        this.printSequenceFile(getWordDictionaryPath());

        System.out.println("\n Step 3: Term Frequency Vectors ");
        this.printSequenceFile(new Path(this.outputFolder +
                "tf-vectors/part-r-00000"));

        System.out.println("\n Step 4: Document Frequency ");
        this.printSequenceFile(new Path(this.outputFolder +
                "tfidf/df-count/part-r-00000"));

        System.out.println("\n Step 5: TFIDF ");
        this.printSequenceFile(getTfIdfVectorsPath());
    }

//    private void fillWordCountMap() {
//        wordCountMap.clear();
//
//        final SequenceFileIterable<Writable, Writable> iterable =
//            new SequenceFileIterable<Writable, Writable>(getWordCountPath(), configuration);
//
//        for (Pair<Writable, Writable> pair : iterable) {
//            wordCountMap.put(pair.getFirst().toString(), Integer.valueOf(pair.getSecond().toString()));
//        }
//    }
//
//    private void fillWordDictionaryMap() {
//        wordDictionaryMap.clear();
//
//        final SequenceFileIterable<Writable, Writable> iterable =
//            new SequenceFileIterable<Writable, Writable>(getWordDictionaryPath(), configuration);
//
//        for (Pair<Writable, Writable> pair : iterable) {
//            wordDictionaryMap.put(pair.getFirst().toString(), Integer.valueOf(pair.getSecond().toString()));
//        }
//    }

    private Path getWordCountPath() {
        return new Path(this.outputFolder + "wordcount/part-r-00000");
    }

    private Path getWordDictionaryPath() {
        return new Path(this.outputFolder, "dictionary.file-0");
    }

    private Path getTfIdfVectorsPath() {
        return new Path(this.outputFolder + "tfidf/tfidf-vectors/part-r-00000");
    }

    private int getWordCount() {
        int count = 0;

        SequenceFileIterable<Writable, Writable> iterable =
                new SequenceFileIterable<Writable, Writable>(getWordDictionaryPath(), configuration);

        for (Pair<Writable, Writable> pair : iterable) {
            count++;
        }

        return count;
    }

    public void writeTfIdfToFileCSV(final String file) throws IOException {
        CSVWriter writer = new CSVWriter(new FileWriter(file), ',');

        SequenceFileIterable<Writable, Writable> iterable =
            new SequenceFileIterable<Writable, Writable>(getTfIdfVectorsPath(), configuration);

        int wordCount = getWordCount();

        for (Pair<Writable, Writable> pair : iterable) {
            String uri = pair.getFirst().toString();

            String vector = pair.getSecond().toString();

            String mainCat = String.valueOf(uriToQuestion.get(uri).getMainCatId());

            writer.writeNext(parseTfIdfVector(uri, vector, wordCount, mainCat));
        }

        writer.close();
    }

    private static String[] parseTfIdfVector(final String uri,
                                             final String vector,
                                             final int wordCount,
                                             final String mainCat) {
        final String[] values = vector.replaceAll("[{}]", "").split(",");

        Map<Integer, Double> indexValueMap = new HashMap<Integer, Double>();

        for (int i = 0; i < values.length; i++) {
            String tokens[] = values[i].split(":");

            int index = Integer.parseInt(tokens[0]);
            double value = Double.parseDouble(tokens[1]);

            indexValueMap.put(index, value);
        }

        // Plus one for main category and one for uri of question
        String[] valueArray = new String[wordCount + 2];

        // First column is uri of question
        valueArray[0] = uri;

        // Last column is main category
        valueArray[wordCount + 1] = mainCat;

        for (int i = 1; i <= wordCount; i++) {
            int index = i - 1;

            if (indexValueMap.containsKey(index)) {
                valueArray[i] = String.valueOf(indexValueMap.get(index));
            } else {
                valueArray[i] = String.valueOf(0);
            }
        }

        return valueArray;
    }

}
