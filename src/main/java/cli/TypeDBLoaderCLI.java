package cli;

import loader.TypeDBLoader;
import picocli.CommandLine;

@CommandLine.Command(description = "Welcome to the CLI of TypeDB Loader - your TypeDB data loading tool", name = "TypeDB Loader", version = "1.0.0-alpha", mixinStandardHelpOptions = true)
public class TypeDBLoaderCLI {

    public static void main(String[] args) {
        LoadOptions options = LoadOptions.parse(args);
        options.print();
        TypeDBLoader loader = new TypeDBLoader(options);
        loader.load();
    }

}

