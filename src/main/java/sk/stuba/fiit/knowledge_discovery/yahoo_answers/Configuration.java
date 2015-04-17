package sk.stuba.fiit.knowledge_discovery.yahoo_answers;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class Configuration {

    private static final Properties props = new Properties();

    private Configuration() {}

    public static void load() throws IOException {
        InputStream input = new FileInputStream("config.properties");
        props.load(input);
    }

    public static boolean getBooleanProperty(final String name) {
        return Boolean.valueOf(props.getProperty(name));
    }

    public static String getStringProperty(final String name) {
        return props.getProperty(name);
    }

}
