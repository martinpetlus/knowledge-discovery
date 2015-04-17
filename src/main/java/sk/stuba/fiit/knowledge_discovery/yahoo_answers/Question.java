package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.util.HashMap;
import java.util.Map;

public final class Question {

    private static int categoryId = 0;

    private static final Map<String, Integer> categoryToId = new HashMap<String, Integer>();

    private final String uri;

    private final String subject;

    private final String content;

    private final String bestAnswer;

    private final String mainCat;

    private final int mainCatId;

    private static boolean appendContent = false;

    private static boolean appendBestAnswer = false;

    public static void setAppendContentToText(final boolean appendContent) {
        Question.appendContent = appendContent;
    }

    public static void setAppendBestAnswerToText(final boolean appendBestAnswer) {
        Question.appendBestAnswer = appendBestAnswer;
    }

    public static final class Builder {

        private String uri;

        private String subject;

        private String content = "";

        private String bestAnswer;

        private String mainCat;

        public void setUri(final String uri) {
            this.uri = uri;
        }

        public void setSubject(final String subject) {
            this.subject = subject;
        }

        public void setContent(final String content) {
            this.content = content;
        }

        public void setBestAnswer(final String bestAnswer) {
            this.bestAnswer = bestAnswer;
        }

        public void setMainCat(final String mainCat) {
            this.mainCat = mainCat;
        }

        public Question build() {
            return new Question(this);
        }
    }

    private Question(final Builder builder) {
        if (builder.mainCat == null) {
            throw new IllegalArgumentException("Illegal main cat: " +
                builder.mainCat + "of question with uri: " + builder.uri);
        }

        this.uri = builder.uri;
        this.subject = Utils.normalizeString(builder.subject);
        this.content = Utils.normalizeString(builder.content);
        this.bestAnswer = Utils.normalizeString(builder.bestAnswer);
        this.mainCat = builder.mainCat;

        Integer catId = Question.categoryToId.get(this.mainCat);

        if (catId == null) {
            Question.categoryToId.put(this.mainCat, categoryId);
            this.mainCatId = Question.categoryId++;
        } else {
            this.mainCatId = catId.intValue();
        }
    }

    public String getContent() {
        return content;
    }

    public String getSubject() {
        return subject;
    }

    public String getUri() {
        return uri;
    }

    public String getMainCat() {
        return mainCat;
    }

    public String getBestAnswer() {
        return bestAnswer;
    }

    public String toText() {
        final StringBuilder sb = new StringBuilder();

        sb.append(subject);

        if (appendContent) {
            sb.append(' ');
            sb.append(content);
        }

        if (appendBestAnswer) {
            sb.append(' ');
            sb.append(bestAnswer);
        }

        return sb.toString().trim();
    }

    public int getMainCatId() {
        return mainCatId;
    }

    public static Map<String, Integer> getCategoryToId() {
        return new HashMap<String, Integer>(Question.categoryToId);
    }

}
