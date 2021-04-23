package processor;

public enum ProcessorType {
    ENTITY,
    ATTRIBUTE,
    RELATION,
    APPEND_ATTRIBUTE,
    NESTED_RELATION,
    ATTRIBUTE_RELATION,
    APPEND_OR_INSERT,
    INVALID;

    @Override
    public String toString() {
        return super.toString().toLowerCase().replaceAll("_", "-");
    }
}
