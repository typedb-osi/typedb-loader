package a_rewrite.util;

import com.vaticle.typedb.client.TypeDB;
import com.vaticle.typedb.client.api.connection.TypeDBClient;
import com.vaticle.typedb.client.api.connection.TypeDBSession;
import com.vaticle.typedb.client.api.connection.TypeDBTransaction;
import com.vaticle.typeql.lang.TypeQL;
import com.vaticle.typeql.lang.query.TypeQLDefine;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static util.Util.loadSchemaFromFile;

public class GraknUtil {

    private static final Logger appLogger = LogManager.getLogger("com.bayer.dt.grami");

    public static TypeDBClient getClient(String graknURI) {
        return TypeDB.coreClient(graknURI);
    }

    public static TypeDBClient getClient(String graknURI, int parallelization) {
        return TypeDB.coreClient(graknURI, parallelization);
    }

    public static TypeDBSession getDataSession(TypeDBClient client, String databaseName) {
        return client.session(databaseName, TypeDBSession.Type.DATA);
    }

    public static TypeDBSession getSchemaSession(TypeDBClient client, String databaseName) {
        return client.session(databaseName, TypeDBSession.Type.SCHEMA);
    }

    private static void createDatabase(TypeDBClient client, String databaseName) {
        client.databases().create(databaseName);
    }

    private static void deleteDatabaseIfExists(TypeDBClient client, String databaseName) {
        if (client.databases().contains(databaseName)) {
            client.databases().get(databaseName).delete();
        }
    }

    public static void cleanAndDefineSchemaToDatabase(TypeDBClient client, String databaseName, String schemaPath) {
        deleteDatabaseIfExists(client, databaseName);
        createDatabase(client, databaseName);
        loadAndDefineSchema(client, databaseName, schemaPath);
    }

    private static void defineToGrakn(TypeDBClient client, String databaseName, String schemaAsString) {
        TypeDBSession schemaSession = getSchemaSession(client, databaseName);
        TypeQLDefine q = TypeQL.parseQuery(schemaAsString);

        TypeDBTransaction writeTransaction = schemaSession.transaction(TypeDBTransaction.Type.WRITE);
        writeTransaction.query().define(q);
        writeTransaction.commit();
        writeTransaction.close();
        schemaSession.close();

        appLogger.info("Defined schema to database <" + databaseName + ">");
    }

    // used by <grami update> command
    public static void loadAndDefineSchema(TypeDBClient client, String databaseName, String schemaPath) {
        String schema = loadSchemaFromFile(schemaPath);
        defineToGrakn(client, databaseName, schema);
    }

}