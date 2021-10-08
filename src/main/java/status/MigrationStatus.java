package status;


public class MigrationStatus {
    private final String conceptName;
    private boolean isCompleted;
    private int migratedRows;

    public MigrationStatus(String conceptName, boolean isCompleted, int migratedRows) {
        this.conceptName = conceptName;
        this.isCompleted = isCompleted;
        this.migratedRows = migratedRows;
    }

    public int getMigratedRows() {
        return migratedRows;
    }

    public void setMigratedRows(int migratedRows) {
        this.migratedRows = migratedRows;
    }

    public boolean isCompleted() {
        return isCompleted;
    }

    public void setCompleted(boolean completed) {
        isCompleted = completed;
    }

    @Override
    public String toString() {
        return "MigrationStatus{" +
                "conceptName='" + conceptName + '\'' +
                ", isCompleted=" + isCompleted +
                ", migratedRows=" + migratedRows +
                '}';
    }
}
