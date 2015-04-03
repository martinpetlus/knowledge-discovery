package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

public final class Utils {

    private Utils() {}

    public static String normalizeString(String s) {
        // Remove html tags
        s = s.replaceAll("<[a-zA-Z]+( *?/)?>", "");

        // I'm -> im, don't -> dont, doesn't -> doesnt, ...
        s = s.replaceAll("([a-zA-Z]+)(')([a-zA-Z]+)", "$1$3");

        return s;
    }

}
