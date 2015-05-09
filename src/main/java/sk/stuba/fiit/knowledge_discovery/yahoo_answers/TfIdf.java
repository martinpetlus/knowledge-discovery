package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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

    public TfIdf() throws IOException {
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
        this.printSequenceFile(new Path(this.outputFolder +
                "wordcount/part-r-00000"));

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

    private void tfIdfVectorsToCSV(final PrintWriter writer) {
        final SequenceFileIterable<Writable, Writable> iterable =
                new SequenceFileIterable<Writable, Writable>(getTfIdfVectorsPath(), configuration);

        for (Pair<Writable, Writable> pair : iterable) {
            final StringBuffer sb = new StringBuffer();

            final String uri = pair.getFirst().toString();

            // First column is uri of question
            sb.append(uri);
            sb.append(',');

            // Next columns are tf-idf word values
            sb.append(parseTfIdfVectorToCSV(pair.getSecond().toString()));

            // Last column is category id
            sb.append(',');
            sb.append(uriToQuestion.get(uri).getMainCatId());

            // Append new line character
            sb.append(System.getProperty("line.separator"));

            writer.print(sb);
        }
    }

    private String parseTfIdfVectorToCSV(final String vector) {
        final StringBuffer sb = new StringBuffer();

        final String[] values = vector.replaceAll("[{}]", "").split(",");

        List<Value> valueList = new ArrayList<Value>();

        for (int i = 0; i < values.length; i++) {
            String tokens[] = values[i].split(":");

            int index = Integer.parseInt(tokens[0]);
            double value = Double.parseDouble(tokens[1]);

            valueList.add(new Value(index, value));
        }

        // Sort by index
        Collections.sort(valueList, Value.CMP);

        final int wordCount = getWordCount();

        int lastIndex = -1;

        for (int i = 0; i < valueList.size(); i++) {
            Value value = valueList.get(i);

            sb.append(getCSVFilledEmptyVectorValues(lastIndex, value.getIndex(), true));

            sb.append(value.getValue());

            if (i + 1 < valueList.size()) {
                sb.append(',');
            }

            lastIndex = value.getIndex();
        }

        sb.append(getCSVFilledEmptyVectorValues(lastIndex, wordCount, false));

        return sb.toString();
    }

    private String getCSVFilledEmptyVectorValues(
            final int lastIndex, final int currentIndex, final boolean appendLastComma) {
        final StringBuffer sb = new StringBuffer();

        for (int i = lastIndex + 1; i < currentIndex; i++) {
            sb.append(0.0);

            if (i + 1 < currentIndex) {
                sb.append(',');
            } else {
                if (appendLastComma) {
                    sb.append(',');
                }
            }
        }

        return sb.toString();
    }

    public void writeTfIdfToFileCSV(final String file) throws FileNotFoundException {
        PrintWriter writer = new PrintWriter(file);

        tfIdfVectorsToCSV(writer);

        writer.close();
    }

    private static final class Value {

        private final int index;

        private final double value;

        public static final Comparator<Value> CMP = new Comparator<Value>() {

            public int compare(final Value value1, final Value value2) {
                return value1.getIndex() - value2.getIndex();
            }

        };

        public Value(final int index, final double value) {
            this.index = index;
            this.value = value;
        }

        public int getIndex() {
            return index;
        }

        public double getValue() {
            return value;
        }

    }

}
