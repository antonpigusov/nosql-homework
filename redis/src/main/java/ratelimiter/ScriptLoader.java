package ratelimiter;

import java.io.InputStream;
import java.nio.charset.Charset;

public class ScriptLoader {

    public static String load(String path) {

        try (InputStream script = ScriptLoader.class.getClassLoader().getResourceAsStream(path)) {

            if (script == null) {
                throw new IllegalArgumentException("Script not found: " + path);
            }

            return new String(script.readAllBytes(), Charset.defaultCharset());

        } catch (Exception e) {
            throw new RuntimeException("Failed to load script: " + path, e);
        }

    }

}
