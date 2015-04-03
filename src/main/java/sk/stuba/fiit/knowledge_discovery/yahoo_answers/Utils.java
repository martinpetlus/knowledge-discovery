package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

public final class Utils {

    private Utils() {}

    public static String normalizeString(String s) {
        // Remove html tags (<br>, <br />, ...)
        s = s.replaceAll("<[a-zA-Z]+( *?/)?>", "");

        // I'm -> Im, don't -> dont, doesn't -> doesnt, ...
        s = s.replaceAll("\\b([a-zA-Z]+)(')([a-zA-Z]+)\\b", "$1$3");

        return s;
    }

}
