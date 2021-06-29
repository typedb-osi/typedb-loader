package a_rewrite.generator;

import com.vaticle.typedb.client.api.connection.TypeDBTransaction;

public interface Generator {
    void write(TypeDBTransaction tx, String[] row) throws Exception;
    char getFileSeparator();
}
