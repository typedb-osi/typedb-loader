package preprocessor;

public class RegexPreprocessor {
    String match;
    String replace;

    public RegexPreprocessor(String match, String replace) {
        this.match = match;
        this.replace = replace;
    }

    public String applyProcessor(String value) {
        return value.replaceAll(match, replace);
    }

}
