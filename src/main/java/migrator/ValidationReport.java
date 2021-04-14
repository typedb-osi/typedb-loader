package migrator;

import java.util.ArrayList;

public class ValidationReport {
    private final String config;
    private boolean valid = true;
    private ArrayList<String> errors = new ArrayList<>();

    public ValidationReport(String config, boolean valid, ArrayList<String> errors) {
        this.config = config;
        this.valid = valid;
        this.errors = errors;
    }

    public String getConfig() {
        return config;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public ArrayList<String> getErrors() {
        return errors;
    }

    public void setErrors(ArrayList<String> errors) {
        this.errors = errors;
    }
}
