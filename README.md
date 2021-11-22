

![TypeDBLoader_icon](https://github.com/bayer-science-for-a-better-life/grami/blob/master/typedbloader.png?raw=true)
---
---
### 
![TypeDB Loader Test](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Test/badge.svg?branch=master)
![TypeDB Loader Build](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Build/badge.svg)
###

---

If your [TypeDB](https://github.com/vaticle/typedb) project
 - has a lot of data
 - and you want/need to focus on schema design, inference, and querying

Use TypeDB Loader to take care of your data migration for you. TypeDB Loader streams data from files and migrates them into TypeDB **at scale**!
 
## Features:
 - Data Input:
    - data is streamed to reduce memory requirements
    - supports any tabular data file with your separator of choice (i.e.: csv, tsv, whatever-sv...)
    - supports gzipped files
    - ignores unnecessary columns
 - [Attribute](), [Entity](), [Relation]() Loading:
    - load required/optional attributes of any TypeDB type (string, boolean, long, double, datetime)
    - load required/optional role players (attribute / entity / relation)
    - load list-like attribute columns as n attributes (recommended procedure until attribute lists are fully supported by TypeDB)
    - load list-like player columns as n players for a relation
    - load entity if not present - if present, either do not write or append attributes
 - [Appending Attributes]() to existing things
 - [Append-Attribute-If-Present-Else-Insert]() for entities
 - Data Validation:
    - validate input data rows and log issues for easy diagnosis input data-related issues (i.e. missing attributes/players, invalid characters...)
 - Configuration Validation:
    - write your configuration with confidence: warnings will display useful information for fine tuning, errors will let you know what you forgot. All BEFORE the database is touched.
 - Performance:
    - parallelized asynchronous writes to TypeDB to make the most of your hardware configuration, optimized with engineers @vaticle
 - Stop/Restart (in re-implementation, currently NOT available):
    - tracking of your migration status to stop/restart, or restart after failure

 - [Basic Column Preprocessing using RegEx's]()

Create a Loading Configuration ([example]()) and use TypeDB Loader
 - as an [executable CLI]() - no coding 
 - in [your own Java project]() - easy API

## How it works:

To illustrate how to use TypeDB Loader, we will use a slightly extended version of the "phone-calls" example [dataset]() and [schema]() from the TypeDB developer documentation:

### Configuration

The configuration file tells TypeDB Loader what things you want to insert for each of your data files and how to do it. 

Here are some example:

 - [Attribute Examples]()
 - [Entity Examples]()
 - [Relation Examples]()
 - [Nested Relation - Match by Attribute(s) Example]()
 - [Nested Relation - Match by Player(s) Example]()
 - [Attribute-Player Relation Example]()
 - [Custom Migration Order Example]()

For detailed documentation, please refer to the [WIKI](https://github.com/bayer-science-for-a-better-life/grami/wiki).

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

[See details here]()

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

[See details here]()


## Step-by-Step Tutorial

A complete tutorial for TypeDB version >= 2.5.0 is in work and will be published asap.

A complete tutorial for TypeDB version >= 2.5.0 is in work and will soon be [on Medium]()

A complete tutorial for TypeDB (Grakn) version < 2.0 can be found [on Medium](https://medium.com/@hkuich/introducing-grami-a-data-migration-tool-for-grakn-d4051582f867).

There is an [example repository](https://github.com/bayer-science-for-a-better-life/grami-example) for your convenience.

## Compatibility

TypeDB Loader version == 1.0.0 is tested for:
- [grakn-core](https://github.com/vaticle/typedb) == 2.5.0

GraMi (former name) version == 0.1.1 is tested for:
- [grakn-core](https://github.com/vaticle/typedb) == 2.0.1

Find the Readme for GraMi for grakn == 2.0.x [here](https://github.com/bayer-science-for-a-better-life/grami/blob/XXXXX/README.md)

GraMi version < 0.1.0 is tested for: 
 - [grakn-core](https://github.com/vaticle/typedb) >= 1.8.2

Find the Readme for GraMi for grakn < 2.0 [here](https://github.com/bayer-science-for-a-better-life/grami/blob/b3d6d272c409d6c40254354027b49f90b255e1c3/README.md)

## Contributions

TypeDB Loader was built @[Bayer AG](https://www.bayer.com/) in the Semantic and Knowledge Graph Technology Group with the support of the engineers @[Grakn Labs](https://github.com/orgs/vaticle/people). Special thanks to @flyingsilverfin.

## Licensing

This repository includes software developed at [Bayer AG](https://www.bayer.com/).  It is released under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
 
## Credits

Icon in banner by [Freepik](https://www.freepik.com") from [Flaticon](https://www.flaticon.com/)
