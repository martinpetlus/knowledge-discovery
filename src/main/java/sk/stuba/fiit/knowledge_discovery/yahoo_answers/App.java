package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.util.List;

public final class App {

    public static void main(String args[]) throws Exception {
        final Loader loader = new Loader();

        Configuration.load();

        final List<Question> questions = loader.load(Configuration.
            getStringProperty("INPUT_FILE"));

        Question.setIncludeContentToText(Configuration.
            getBooleanProperty("INCLUDE_CONTENT"));

        Question.setIncludeBestAnswerToText(Configuration.
            getBooleanProperty("INCLUDE_BEST_ANSWER"));

        final TfIdf tfIdf = new TfIdf(Integer.valueOf(Configuration.
            getIntProperty("MINIMUM_WORD_DOCUMENT_FREQUENCY")));

        tfIdf.putQuestions(questions);
        tfIdf.calculateTfIdf();
        tfIdf.printAll();

        tfIdf.writeTfIdfToFileCSV(Configuration.getStringProperty("OUTPUT_FILE"));
    }

}
