package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.util.List;

public final class App {

    public static void main(String args[]) throws Exception {
        Loader loader = new Loader();

        List<Question> questions = loader.load("data/manner-snippet-1.xml");

        TfIdf tfIdf = new TfIdf();

        tfIdf.addQuestions(questions);
        tfIdf.calculateTfIdf();
        tfIdf.printAll();
    }

}
