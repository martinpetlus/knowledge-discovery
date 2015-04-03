package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

import java.io.IOException;
import java.util.List;

// http://technobium.com/tfidf-explained-using-apache-mahout/
public final class TfIdf {

    private String outputFolder;

    private Configuration configuration;

    private FileSystem fileSystem;

    private Path documentsSequencePath;

    private Path tokenizedDocumentsPath;

    private Path tfidfPath;

    private Path termFrequencyVectorsPath;

    public TfIdf() throws IOException {

        configuration = new Configuration();
        fileSystem = FileSystem.get(configuration);

        outputFolder = "output/";
        documentsSequencePath = new Path(outputFolder, "sequence");
        tokenizedDocumentsPath = new Path(outputFolder,
                DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
        tfidfPath = new Path(outputFolder + "tfidf");
        termFrequencyVectorsPath = new Path(outputFolder
                + DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
    }

    public void addQuestions(final List<Question> questions) throws IOException {
        SequenceFile.Writer writer = new SequenceFile.Writer(fileSystem,
                configuration, documentsSequencePath, Text.class, Text.class);

        for (Question question : questions) {
            Text id = new Text(question.getUri());
            Text text = new Text(question.toText());
            writer.append(id, text);
        }

        writer.close();
    }

    public void calculateTfIdf() throws ClassNotFoundException, IOException,
            InterruptedException {

        // Tokenize the documents using Apache Lucene StandardAnalyzer
        DocumentProcessor.tokenizeDocuments(documentsSequencePath,
                StandardAnalyzer.class, tokenizedDocumentsPath, configuration);

        DictionaryVectorizer.createTermFrequencyVectors(tokenizedDocumentsPath,
                new Path(outputFolder),
                DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER,
                configuration, 1, 1, 0.0f, PartialVectorMerger.NO_NORMALIZING,
                true, 1, 100, false, false);

        Pair<Long[], List<Path>> documentFrequencies = TFIDFConverter.
                calculateDF(termFrequencyVectorsPath, tfidfPath,
                        configuration, 100);

        TFIDFConverter.processTfIdf(termFrequencyVectorsPath, tfidfPath,
                configuration, documentFrequencies, 1, 100,
                PartialVectorMerger.NO_NORMALIZING, false, false, false, 1);
    }

    private void printSequenceFile(final Path path) {
        SequenceFileIterable<Writable, Writable> iterable = new SequenceFileIterable<Writable, Writable>(
                path, configuration);
        for (Pair<Writable, Writable> pair : iterable) {
            System.out
                    .format("%20s > %s\n", pair.getFirst(), pair.getSecond());
        }
    }

    public void printAll() {
        this.printSequenceFile(this.documentsSequencePath);

        System.out.println("\n Step 1: Word count ");
        this.printSequenceFile(new Path(this.outputFolder
                + "wordcount/part-r-00000"));

        System.out.println("\n Step 2: Word dictionary ");
        this.printSequenceFile(new Path(this.outputFolder,
                "dictionary.file-0"));

        System.out.println("\n Step 3: Term Frequency Vectors ");
        this.printSequenceFile(new Path(this.outputFolder
                + "tf-vectors/part-r-00000"));

        System.out.println("\n Step 4: Document Frequency ");
        this.printSequenceFile(new Path(this.outputFolder
                + "tfidf/df-count/part-r-00000"));

        System.out.println("\n Step 5: TFIDF ");
        this.printSequenceFile(new Path(this.outputFolder
                + "tfidf/tfidf-vectors/part-r-00000"));
    }

}
