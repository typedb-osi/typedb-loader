package processor;

public enum AttributeValueType {
    STRING,
    LONG,
    DOUBLE,
    BOOLEAN,
    DATETIME,
    INVALID;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
