package processor;

public enum ProcessorTypes {
    entity,
    attribute,
    relation,
    append_attribute,
    nested_relation,
    attribute_relation,
    append_or_insert;

    @Override
    public String toString() {
        return super.toString().replaceAll("_", "-");
    }
}
