/*
 * Copyright (C) 2021 Bayer AG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.vaticle.typedb.osi.loader.cli;

import picocli.CommandLine;

import javax.annotation.Nullable;

@CommandLine.Command(name = "load", description = "load data and/or schema", mixinStandardHelpOptions = true)
public class LoadOptions {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--config"}, description = "config file in JSON format", required = true)
    public String dataConfigFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your TypeDB instance", required = true)
    public String databaseName;

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "Connect to TypeDB Core server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    public String typedbURI;

    @CommandLine.Option(names = {"-tdbc", "--typedb-cluster"}, description = "Connect to TypeDB Cluster instead of TypeDB Core. Specify a cluster server with 'server:port'.")
    public String typedbClusterURI;

    @CommandLine.Option(names = {"--username"}, description = "Username")
    public @Nullable String username;

    @CommandLine.Option(names = {"--password"}, description = "Password", interactive = true, arity = "0..1")
    public @Nullable String password;

    @CommandLine.Option(names = {"--tls-enabled"}, description = "Connect to TypeDB Cluster with TLS encryption")
    public boolean tlsEnabled;

    @CommandLine.Option( names = {"--tls-root-ca"}, description = "Path to the TLS root CA file")
    public @Nullable String tlsRootCAPath;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists", defaultValue = "false")
    public boolean cleanMigration;

    @CommandLine.Option(names = {"-ls", "--loadSchema"}, description = "optional - reload schema when continuing a migration (ignored when clean migration)", defaultValue = "false")
    public boolean loadSchema;

    @CommandLine.Option(names = {"-mi", "--allowMultiInsert"}, description = "Allow match-inserts to match multiple answers and insert for each.", defaultValue = "false")
    public boolean multiInsert;

    public static LoadOptions parse(String[] args) {
        CommandLine commandLine = new CommandLine(new TypeDBLoaderCLI())
                .addSubcommand("load", new LoadOptions());
        if (args.length == 0) {
            commandLine.usage(commandLine.getOut());
            System.exit(0);
        }

        CommandLine.ParseResult arguments = commandLine.parseArgs(args);
        if (arguments.isUsageHelpRequested()) {
            commandLine.usage(commandLine.getOut());
            System.exit(0);
        } else if (arguments.isVersionHelpRequested()) {
            commandLine.printVersionHelp(commandLine.getOut());
            System.exit(0);
        } else if (arguments.subcommand().isUsageHelpRequested()) {
            commandLine.getSubcommands().get("load").usage(commandLine.getOut());
            System.exit(0);
        } else if (!arguments.hasSubcommand()) {
            commandLine.getErr().println("Missing subcommand");
            commandLine.usage(commandLine.getOut());
            System.exit(0);
        } else {
            return arguments.subcommand().asCommandLineList().get(0).getCommand();
        }
        throw new RuntimeException("Illegal state");
    }

    public void print() {
        spec.commandLine().getOut().println("############## TypeDB Loader ###############");
        spec.commandLine().getOut().println("TypeDB Loader started with parameters:");
        spec.commandLine().getOut().println("\tconfiguration: " + dataConfigFilePath);
        spec.commandLine().getOut().println("\tdatabase name: " + databaseName);
        if (typedbClusterURI != null) {
            spec.commandLine().getOut().println("\tTypeDB cluster URI: " + typedbClusterURI);
            spec.commandLine().getOut().println("\tTypeDB cluster username: " + username);
            spec.commandLine().getOut().println("\tTypeDB cluster TLS enabled: " + tlsEnabled);
            spec.commandLine().getOut().println("\tTypeDB cluster TLS path: " + (tlsRootCAPath == null ? "N/A" : tlsRootCAPath));
        } else {
            spec.commandLine().getOut().println("\tTypeDB server URI: " + typedbURI);
        }
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);
        spec.commandLine().getOut().println("\treload schema (if not doing clean migration): " + loadSchema);
    }
}