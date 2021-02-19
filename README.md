

![grami_icon](https://github.com/bayer-science-for-a-better-life/grami/blob/master/grami_banner.png?raw=true)
---
---
### 
![GraMi Test](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Test/badge.svg?branch=master)
![GraMi Build](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Build/badge.svg)
###

---

If your [Grakn.ai](https://github.com/graknlabs/grakn) project
 - has a lot of data
 - and you want/need to focus on schema design, inference, and querying

Use GraMi (**Gra**kn**Mi**grator) to take care of your data migration for you. GraMi streams data from files and migrates them into grakn **at scale**!
 
## Features:
 - Data Input:
    - data is streamed to reduce memory requirements
    - supports any tabular data file with your separator of choice (i.e.: csv, tsv, whatever-sv...)
    - supports gzipped files
    - ignores unnecessary columns
 - [Entity](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Entities), [Relation](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Relations), and [Nested Relations](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Nested-Relations) Migration:
    - migrate required/optional attributes of any grakn type (string, boolean, long, double, datetime)
    - migrate required/optional role players (entity & relations)
    - migrate list-like attribute columns as n attributes (recommended procedure until attribute lists are fully supported by Grakn)
    - migrate list-like player columns as n players
 - Data Validation:
    - validate input data rows and log issues for easy diagnosis input data-related issues (i.e. missing attributes/players, invalid characters...)
 - Performance:
    - parallelized asynchronous writes to Grakn to make the most of your hardware configuration
 - Stop/Restart:
    - tracking of your migration status to stop/restart, or restart after failure
 - [Schema Updating](https://github.com/bayer-science-for-a-better-life/grami/wiki/Schema-Updating) for non-breaking changes (i.e. add to your schema or modify concepts that do not yet contain any data)
 - [Appending Attributes](https://github.com/bayer-science-for-a-better-life/grami/wiki/Append-Attributes) to existing things
 - [Basic Column Preprocessing using RegEx's](https://github.com/bayer-science-for-a-better-life/grami/wiki/Preprocessing)

After creating your processor configuration ([example](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/processorConfig.json)) and data configuration ([example](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/dataConfig.json)), you can use GraMi
 - as an [executable CLI](https://github.com/bayer-science-for-a-better-life/grami/wiki/Grami-as-Executable-CLI) - no coding - configuration required 
 - in [your own Java project](https://github.com/bayer-science-for-a-better-life/grami/wiki/GraMi-as-Dependency) - easy API - configuration required
 
Please note that the recommended way of developing your schema is still to use your favorite code editor/IDE in combination with the grakn console.

## How it works:

To illustrate how to use GraMi, we will use a slightly extended version of the "phone-calls" example [dataset](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls) and [schema](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/schema.gql) from the grakn developer documentation:

### Processor Configuration

The processor configuration file describes how you want data to be migrated given the constraints of your schema. There are different processor types. 

Depending on what you would like to migrate, see here:

 - [Entity Processor Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Entities#processor-config)
 - [Relation Processor Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Relations#processor-config)
 - [Nested Relation Processor Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Nested-Relations#processor-config)

### Data Configuration

The data configuration file maps each data file and its columns to your processor configurations and specifies whether a column contains single/a list of values. In addition, you can specify the size of processing batches and the number of threads (the number of cores on your machine) to be used for the migration to fine-tune the performance (where a greater number of threads up to 8 is always better while batchsize depends on the complexity of your concept (# of attributes per entity record and/or # of players per relation record)). 

A good point to start the performance optimization is to set the number of threads equal to the number of cores on your machine and the batchsize to 500 * threads (i.e.: 4 threads => batchsize = 2000).

See Example here:

 - [Entity Data Config Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Entities#data-config)
 - [Relation Data Config Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Relations#data-config)
 - [Nested Relation - Match by Attribute(s) - Data Config Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Nested-Relations#data-config---attribute-matching)
 - [Nested Relation - Match by Player(s) - Data Config Example](https://github.com/bayer-science-for-a-better-life/grami/wiki/Migrating-Nested-Relations#data-config---player-matching)

### Migrate Data

Once your configuration files are complete, you can use GraMi in one of two ways:

 1. As an executable command line interface - no coding required

```Shell
./bin/grami migrate \
-dc /path/to/dataConfig.json \
-pc /path/to/processorConfig.json \
-ms /path/to/migrationStatus.json \
-s /path/to/schema.gql \
-db yourFavoriteDatabase
```

[See details here](https://github.com/bayer-science-for-a-better-life/grami/wiki/Grami-as-Executable-CLI)

 2. As a dependency in your own Java code

```Java
public class Migration {

    private static final String schema = "/path/to/your/schema.gql";
    private static final String processorConfig = "/path/to/your/processorConfig.json";
    private static final String dataConfig = "/path/to/your/dataConfig.json";
    private static final String migrationStatus = "/path/to/your/migrationStatus.json";

    private static final String graknURI = "127.0.0.1:1729";               // defines which grakn server to migrate into
    private static final String databaseName = "yourFavoriteDatabase";      // defines which keyspace to migrate into

    private static final MigrationConfig migrationConfig = new MigrationConfig(graknURI, databaseName, schema, dataConfig, processorConfig);

    public static void main(String[] args) throws IOException {
        GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatus, true);
        mig.migrate(true, true, true, true);
    }
}
```

[See details here](https://github.com/bayer-science-for-a-better-life/grami/wiki/GraMi-as-Dependency)


## Step-by-Step Tutorial

A complete tutorial for grakn version >= 2.0 is in work and will be published asap.

A complete tutorial for grakn version >= 1.8.2, but < 2.0 can be found [on Medium](https://medium.com/@hkuich/introducing-grami-a-data-migration-tool-for-grakn-d4051582f867).

There is this [example repository](https://github.com/bayer-science-for-a-better-life/grami-example).

## Compatibility

GraMi version >= 0.1.0 is tested for:
- [grakn-core](https://github.com/graknlabs/grakn) >= 2.0-alpha-6
- [client-java](https://github.com/graknlabs/client-java) >= 2.0.0-alpha-8

GraMi version < 0.1.0 is tested for: 
 - [grakn-core](https://github.com/graknlabs/grakn) >= 1.8.2 
 - [client-java](https://github.com/graknlabs/client-java) >= 1.8.3

Find the Readme for GraMi for grakn < 2.0 [here](https://github.com/bayer-science-for-a-better-life/grami/blob/b3d6d272c409d6c40254354027b49f90b255e1c3/README.md)

## Contributions

GraknMigrator was built @[Bayer AG](https://www.bayer.com/) in the Semantic and Knowledge Graph Technology Group with the support of the engineers @[Grakn Labs](https://github.com/orgs/graknlabs/people)

## Licensing

This repository includes software developed at [Bayer AG](https://www.bayer.com/).  It is released under a [GNU-3 General Public License](https://www.gnu.org/licenses/gpl-3.0.de.html).
 
## Credits

Icon in banner by [Smashicons](https://www.flaticon.com/de/autoren/smashicons) from [Flaticon](https://www.flaticon.com/)
