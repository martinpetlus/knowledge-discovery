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

    private Map<String, Question> uriToQuestion;

    private final int minimumWordOccurrence;

    public TfIdf(final int minimumWordOccurrence) throws IOException {
        this.minimumWordOccurrence = minimumWordOccurrence;

        this.configuration = new Configuration();
        this.fileSystem = FileSystem.get(this.configuration);

        this.outputFolder = "output/";
        this.documentsSequencePath = new Path(this.outputFolder, "sequence");
        this.tokenizedDocumentsPath = new Path(this.outputFolder,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);

        this.tfIdfPath = new Path(outputFolder + "tfidf");
        this.termFrequencyVectorsPath = new Path(this.outputFolder +
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

    private Map<Integer, Integer> getWordIndexCountMap() {
        Map<String, Integer> wordCountMap = getWordCountMap();

        Map<String, Integer> wordDictionaryMap = getWordDictionaryMap();

        Map<Integer, Integer> wordIndexCountMap = new HashMap<Integer, Integer>();

        for (Map.Entry<String, Integer> entry : wordDictionaryMap.entrySet()) {
            String word = entry.getKey();

            Integer wordIndex = entry.getValue();

            wordIndexCountMap.put(wordIndex, wordCountMap.get(word));
        }

        return wordIndexCountMap;
    }

    private Map<String, Integer> getWordCountMap() {
        Map<String, Integer> wordCountMap = new HashMap<String, Integer>();

        SequenceFileIterable<Writable, Writable> iterable =
            new SequenceFileIterable<Writable, Writable>(getWordCountPath(), configuration);

        for (Pair<Writable, Writable> pair : iterable) {
            wordCountMap.put(pair.getFirst().toString(), Integer.valueOf(pair.getSecond().toString()));
        }

        return wordCountMap;
    }

    private Map<String, Integer> getWordDictionaryMap() {
        Map<String, Integer> wordDictionaryMap = new HashMap<String, Integer>();

        SequenceFileIterable<Writable, Writable> iterable =
            new SequenceFileIterable<Writable, Writable>(getWordDictionaryPath(), configuration);

        for (Pair<Writable, Writable> pair : iterable) {
            wordDictionaryMap.put(pair.getFirst().toString(), Integer.valueOf(pair.getSecond().toString()));
        }

        return wordDictionaryMap;
    }

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

        Map<Integer, Integer> wordIndexCountMap = getWordIndexCountMap();

        for (Pair<Writable, Writable> pair : iterable) {
            String uri = pair.getFirst().toString();

            String vector = pair.getSecond().toString();

            String mainCat = String.valueOf(uriToQuestion.get(uri).getMainCatId());

            writer.writeNext(parseTfIdfVector(uri, vector, wordCount, mainCat, wordIndexCountMap));
        }

        writer.close();
    }

    private String[] parseTfIdfVector(final String uri,
                                      final String vector,
                                      final int wordCount,
                                      final String mainCat,
                                      final Map<Integer, Integer> wordIndexCountMap) {
        final String[] values = vector.replaceAll("[{}]", "").split(",");

        Map<Integer, Double> indexValueMap = new HashMap<Integer, Double>();

        for (int i = 0; i < values.length; i++) {
            String tokens[] = values[i].split(":");

            int index = Integer.parseInt(tokens[0]);
            double value = Double.parseDouble(tokens[1]);

            indexValueMap.put(index, value);
        }

        List<String> valueList = new ArrayList<String>();

        // First column is uri of question
        valueList.add(uri);

        for (int wordIndex = 0; wordIndex < wordCount; wordIndex++) {
            // Skip words (columns) with less minimum occurrence
            if (wordIndexCountMap.get(wordIndex) < minimumWordOccurrence) {
                continue;
            }

            if (indexValueMap.containsKey(wordIndex)) {
                valueList.add(String.valueOf(indexValueMap.get(wordIndex)));
            } else {
                valueList.add(String.valueOf(0));
            }
        }

        // Last column is main category
        valueList.add(mainCat);

        return valueList.toArray(new String[valueList.size()]);
    }

}
