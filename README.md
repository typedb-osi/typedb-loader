![TypeDBLoader_icon](https://github.com/bayer-science-for-a-better-life/grami/blob/master/typedbloader.png?raw=true)
---
---

###  

[![TypeDB Loader Test](https://github.com/bayer-science-for-a-better-life/grami/actions/workflows/testandbuild.yaml/badge.svg)](https://github.com/bayer-science-for-a-better-life/grami/actions/workflows/testandbuild.yaml)
[![TypeDB Loader Build](https://github.com/bayer-science-for-a-better-life/grami/actions/workflows/release.yaml/badge.svg)](https://github.com/bayer-science-for-a-better-life/grami/actions/workflows/release.yaml)

###

---

If your [TypeDB](https://github.com/vaticle/typedb) project

- has a lot of data
- and you want/need to focus on schema design, inference, and querying

Use TypeDB Loader to take care of your data migration for you. TypeDB Loader streams data from files and migrates them
into TypeDB **at scale**!

## Features:

- Data Input:
    - data is streamed to reduce memory requirements
    - supports any tabular data file with your separator of choice (i.e.: csv, tsv, whatever-sv...)
    - supports gzipped files
    - ignores unnecessary columns
- [Attribute](https://github.com/typedb-osi/typedb-loader/wiki/02-Loading-Attributes), [Entity](https://github.com/typedb-osi/typedb-loader/wiki/03-Loading-Entities), [Relation](https://github.com/typedb-osi/typedb-loader/wiki/04-Loading-Relations)
  Loading:
    - load required/optional attributes of any TypeDB type (string, boolean, long, double, datetime)
    - load required/optional role players (attribute / entity / relation)
    - load list-like attribute columns as n attributes (recommended procedure until attribute lists are fully supported
      by TypeDB)
    - load list-like player columns as n players for a relation
    - load entity if not present - if present, either do not write or append attributes
- [Appending Attributes](https://github.com/typedb-osi/typedb-loader/wiki/05-Appending-Attributes) to existing things
- [Append-Attribute-Or-Insert-Entity](https://github.com/typedb-osi/typedb-loader/wiki/06-Append-Or-Insert) for entities
- Data Validation:
    - validate input data rows and log issues for easy diagnosis input data-related issues (i.e. missing
      attributes/players, invalid characters...)
- Configuration Validation:
    - write your configuration with confidence: warnings will display useful information for fine tuning, errors will
      let you know what you forgot. All BEFORE the database is touched.
- Performance:
    - parallelized asynchronous writes to TypeDB to make the most of your hardware configuration, optimized with
      engineers @vaticle
- Stop/Restart (in re-implementation, currently NOT available):
    - tracking of your migration status to stop/restart, or restart after failure

- [Basic Column Preprocessing using RegEx's](https://github.com/typedb-osi/typedb-loader/wiki/08-Preprocessing)

Create a Loading
Configuration ([example](https://github.com/typedb-osi/typedb-loader/blob/master/src/test/resources/phoneCalls/config.json))
and use TypeDB Loader

- as an [executable CLI](https://github.com/typedb-osi/typedb-loader/wiki/10-TypeDB-Loader-as-Executable-CLI) - no
  coding
- in [your own Java project](https://github.com/typedb-osi/typedb-loader/wiki/09-TypeDB-Loader-as-Dependency) - easy API

## How it works:

To illustrate how to use TypeDB Loader, we will use a slightly extended version of the "phone-calls"
example [dataset](https://github.com/typedb-osi/typedb-loader/tree/master/src/test/resources/phoneCalls)
and [schema](https://github.com/typedb-osi/typedb-loader/blob/master/src/test/resources/phoneCalls/schema.gql) from the
TypeDB developer documentation:

### Configuration

The configuration file tells TypeDB Loader what things you want to insert for each of your data files and how to do it.

Here are some example:

- [Attribute Examples](https://github.com/typedb-osi/typedb-loader/wiki/02-Loading-Attributes)
- [Entity Examples](https://github.com/typedb-osi/typedb-loader/wiki/03-Loading-Entities)
- [Relation Examples](https://github.com/typedb-osi/typedb-loader/wiki/04-Loading-Relations)
- [Nested Relation - Match by Attribute(s) Example](https://github.com/typedb-osi/typedb-loader/wiki/04-Loading-Relations#loading-relations-with-entityrelation-players-matched-on-attribute-ownerships-incl-nested-relations)
- [Nested Relation - Match by Player(s) Example](https://github.com/typedb-osi/typedb-loader/wiki/04-Loading-Relations#loading-relations-relation-players-matching-on-players-in-playing-relation-incl-nested-relations)
- [Attribute-Player Relation Example](https://github.com/typedb-osi/typedb-loader/wiki/04-Loading-Relations#loading-relations-with-attribute-players)
- [Custom Migration Order Example](https://github.com/typedb-osi/typedb-loader/wiki/07-Custom-Load-Order)

For detailed documentation, please refer to the [WIKI](https://github.com/bayer-science-for-a-better-life/grami/wiki).

The [config](https://github.com/typedb-osi/typedb-loader/tree/master/src/test/resources/phoneCalls/config.json) in the
phone-calls test is a good starting example of a configuration.

### Migrate Data

Once your configuration files are complete, you can use TypeDB Loader in one of two ways:

1. As an executable command line interface - no coding required:

```Shell
./bin/typedbloader load \
                -tdb localhost:1729, \
                -c /path/to/your/config.json \
                -db databaseName \
                -cm
```

[See details here](https://github.com/typedb-osi/typedb-loader/wiki/10-TypeDB-Loader-as-Executable-CLI)

2. As a dependency in your own Java code:

```Java
public class LoadingData {

    public void loadData() {
        String uri = "localhost:1729";
        String config = "path/to/your/config.json";
        String database = "databaseName";

        String[] args = {
                "load",
                "-tdb", uri,
                "-c", config,
                "-db", database,
                "-cm"
        };

        LoadOptions options = LoadOptions.parse(args);
        TypeDBLoader loader = new TypeDBLoader(options);
        loader.load();
    }
}
```

[See details here](https://github.com/typedb-osi/typedb-loader/wiki/09-TypeDB-Loader-as-Dependency)

## Step-by-Step Tutorial

A complete tutorial for TypeDB version >= 2.5.0 is in work and will be published asap.

An example of configuration and usage of TypeDB Loader on real data can be
found [in the TypeDB Examples](https://github.com/vaticle/typedb-examples/tree/master/biology/catalogue_of_life).

A complete tutorial for TypeDB (Grakn) version < 2.0 can be
found [on Medium](https://medium.com/@hkuich/introducing-grami-a-data-migration-tool-for-grakn-d4051582f867).

There is an [example repository](https://github.com/bayer-science-for-a-better-life/grami-example) for your convenience.

## Connecting to TypeDB Cluster

To connect to TypeDB Cluster, a set of options is provided:
```
--typedb-cluster=<address:port>
--username=<username>
--password // can be asked for interactively
--tls-enabled
--tls-root-ca=<path/to/CA/cert>
```

## Compatibility Table

Ranges are [inclusive, exclusive).

| TypeDB Loader  | TypeDB Client (internal) |      TypeDB      |  TypeDB Cluster  |
|:--------------:|:------------------------:|:----------------:|:----------------:|
|     1.6.0      |          2.14.2          | 2.14.x to 2.16.x | 2.14.x to 2.16.x |
| 1.2.0 to 1.6.0 |      2.8.0 - 2.14.0      | 2.8.0 to 2.14.0  |       N/A        |
| 1.1.0 to 1.2.0 |          2.8.0           |      2.8.x       |       N/A        |
|     1.0.0      |      2.5.0 to 2.7.1      |  2.5.x to 2.7.x  |       N/A        |
|     0.1.1      |      2.0.0 to 2.5.0      |  2.0.x to 2.4.x  |       N/A        |
|      <0.1      |          1.8.0           |      1.8.x       |       N/A        |

* [Type DB](https://github.com/vaticle/typedb)

Find the Readme for GraMi for grakn < 2.0 [here](https://github.com/bayer-science-for-a-better-life/grami/blob/b3d6d272c409d6c40254354027b49f90b255e1c3/README.md)

## Contributions

TypeDB Loader was built @[Bayer AG](https://www.bayer.com/) in the Semantic and Knowledge Graph Technology Group with
the support of the engineers @[Vaticle](https://github.com/vaticle).

## Licensing

This repository includes software developed at [Bayer AG](https://www.bayer.com/). It is released under
the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Credits

Icon in banner by [Freepik](https://www.freepik.com") from [Flaticon](https://www.flaticon.com/)
