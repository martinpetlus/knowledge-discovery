package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

public final class Question {

    private final String uri;

    private final String subject;

    private final String content;

    private final String bestAnswer;

    private final String mainCat;

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
            throw new IllegalArgumentException("Illegal main cat: " + builder.mainCat);
        }

        this.uri = builder.uri;
        this.subject = Utils.normalizeString(builder.subject);
        this.content = Utils.normalizeString(builder.content);
        this.bestAnswer = Utils.normalizeString(builder.bestAnswer);
        this.mainCat = builder.mainCat;
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
        return content + " " + subject;
    }


}
