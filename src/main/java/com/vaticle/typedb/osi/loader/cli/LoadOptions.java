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

@CommandLine.Command(name = "load", description = "load data and/or schema", mixinStandardHelpOptions = true)
public class LoadOptions {
    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;

    @CommandLine.Option(names = {"-c", "--config"}, description = "config file in JSON format", required = true)
    public String dataConfigFilePath;

    @CommandLine.Option(names = {"-db", "--database"}, description = "target database in your grakn instance", required = true)
    public String databaseName;

    @CommandLine.Option(names = {"-tdb", "--typedb"}, description = "optional - TypeDB server in format: server:port (default: localhost:1729)", defaultValue = "localhost:1729")
    public String typedbURI;

    @CommandLine.Option(names = {"-cm", "--cleanMigration"}, description = "optional - delete old schema and data and restart migration from scratch - default: continue previous migration, if exists", defaultValue = "false")
    public boolean cleanMigration;

    @CommandLine.Option(names = {"-ls", "--loadSchema"}, description = "optional - reload schema when continuing a migration (ignored when clean migration)", defaultValue = "false")
    public boolean loadSchema;

    public static LoadOptions parse(String[] args) {
        CommandLine commandLine = new CommandLine(new TypeDBLoaderCLI())
                .addSubcommand("load", new LoadOptions());
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
        spec.commandLine().getOut().println("\tTypeDB server: " + typedbURI);
        spec.commandLine().getOut().println("\tdelete database and all data in it for a clean new migration?: " + cleanMigration);
        spec.commandLine().getOut().println("\treload schema (if not doing clean migration): " + loadSchema);
    }
}