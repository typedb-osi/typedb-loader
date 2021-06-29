package a_rewrite.config;

public enum TypeHandler {
    ENTITY,
    RELATION,
    ATTRIBUTE,
    OWNERSHIP;

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }
}
