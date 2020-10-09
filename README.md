

![grami_icon](https://github.com/bayer-science-for-a-better-life/grami/blob/master/grami_banner.png?raw=true)
---
---
### 
![GraMi Test](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Test/badge.svg?branch=master)
![GraMi Build](https://github.com/bayer-science-for-a-better-life/grami/workflows/GraMi%20Build/badge.svg?branch=master)
###

---

If your [Grakn.ai](https://github.com/graknlabs/grakn) project
 - has a lot of data
 - and you want/need to focus on schema design, inference, and querying

Use GraMi (**Gra**kn**Mi**grator) to take care of your data loading for you. GraMi streams data from files and migrates them into grakn **at scale**!
 
## Features:
 - Data Input:
    - data is streamed to reduce memory requirements
    - supports any tabular data file with your separator of choice (i.e.: csv, tsv, whatever-sv...)
    - supports gzipped files
    - ignore unnecessary columns
 - Entity and Relation Migration:
    - migrate required/optional attributes of any grakn type (string, boolean, long, double, datetime)
    - migrate required/optional role players (entity, relation & attributes planned)
    - migrate list-like attribute columns as n attributes (recommended procedure until attribute lists are fully supported by Grakn)
 - Data Validation:
    - validate input data rows and log issues for easy diagnosis input data-related issues (i.e. missing attributes/players, invalid characters...)
 - Performance:
    - parallelized asynchronous writes to Grakn to make the most of your hardware configuration
 - Stop/Restart:
    - tracking of your migration status to stop/restart, or restart after failure

After [creating your processor configuration](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/processorConfig.json) and [data configuration](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/dataConfig.json), you can use GraMi
 - as a [Command Line Application](https://github.com/bayer-science-for-a-better-life/grami/releases) - no coding - configuration required 
 - in [your own Java project](https://github.com/bayer-science-for-a-better-life/grami#using-grami-in-your-java-application) - easy API - configuration required
 
Please note that the recommended way of developing your schema is still to use your favorite code editor/IDE in combination with the grakn console.

## How it works:

To illustrate how to use GraMi, we will use a slightly extended version of the "phone-calls" example [dataset](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls) and [schema](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/schema.gql) from Grakn:

#### Processor Configuration

The processor configuration file describes how you want data to be migrated according to your schema. There are two difference processor types - one for entities and one for relations. 

To get started, define the "processors" list in your processor configuration file:

```JSON
{
  "processors": [
  ]
}
```

##### Entity Processors

For each entity in your schema, define a processor object that specifies for each entity attribute
  - its concept name
  - its value type
  - whether it is required 
 
Please note that for each entity, at least one attribute should be required to avoid inserting empty entites into grakn. All attributes declared as keys should also be required. 

For example, given the following entity in your schema:
 
 ```GraphQL
person sub entity,
    plays customer,
    plays caller,
    plays callee,
    has first-name,
    has last-name,
    has phone-number,
    has city,
    has age,
    has nick-name;
 ```

Add the following processor object:

```
{
    "processor": "person",                  // the ID of your processor
    "processorType": "entity",              // creates an entity
    "schemaType": "person",                 // of type person
    "conceptGenerators": {
        "attributes": {                             // with the following attributes according to schema
            "first-name": {                             // ID of attribute generator
                "attributeType": "first-name",              // creates "first-name" attributes
                "valueType": "string",                      // of value type string (other possibilities: long, double, boolean, or datetime)
                "required": false                           // which is not required for each data record
            },
            < lines omitted >
            "phone-number": {                           // ID of attribute generator
                "attributeType": "phone-number",            // creates "phone-number" attributes
                "valueType": "string",                      // of value type string
                "required": true                            // which is required for each data record
            },
            < lines omitted >
            "nick-name": {                              // ID of attribute generator
                "attributeType": "nick-name",               // creates "phone-number" attributes
                "valueType": "string",                      // of value type string
                "required": false                           // which is required for each data record
            }
        }
    }
}
```

Do above for all entities and their attributes in the schema. GraMi will ensure that all values in your data files adhere to the value type specified or try to cast them. GraMi will also ensure that no data records enter grakn that are incomplete (missing required attributes).

##### Relation Processors

For each relation in your schema, define a processor object that specifies
  - each relation attribute, its value type, and whether it is required
  - each relation player entity type, role, identifying attribute in the data file and its value type, as well as whether the player is required

For example, given the following relation in your schema:

 ```GraphQL
call sub relation,
    relates caller,
    relates callee,
    has started-at,
    has duration;
 ```

Add the following processor object:

```
{
    "processor": "call",                                // the ID of your processor
    "processorType": "relation",                        // creates a relation
    "schemaType": "call",                               // of type call
    "conceptGenerators": {
        "players": {                                        // with the following players according to schema
            "caller": {                                         // ID of player generator
                "playerType": "person",                             // matches entity of type person
                "uniquePlayerId": "phone-number",                   // using attribute phone-number as unique identifier for type person
                "idValueType": "string",                            // of value type string
                "roleType": "caller",                               // inserts person as player the role caller
                "required": true                                    // which is a required role for each data record
                
            },
            "callee": {                                         // ID of player generator
                "playerType": "person",                             // matches entity of type person
                "uniquePlayerId": "phone-number",                   // using attribute phone-number as unique identifier for type person
                "idValueType": "string",                            // of value type string
                "roleType": "calle",                                // inserts person as player the role callee
                "required": true                                    // which is a required role for each data record                
            }
        },
        "attributes": {                                     // with the following attributes according to schema
            "started-at": {                                     // ID of attribute generator
                "attributeType": "started-at",                      // creates "started-at" attributes
                "valueType": "datetime",                            // of value type datetime
                "required": true                                    // which is required for each data record
            },
            "duration": {                                       // ID of attribute generator
                "attributeType": "duration",                        // creates "duration" attributes
                "valueType": "long",                                // of value type long
                "required": true                                    // which is required for each data record
            }
        }
    }
}
```

Do above for all relations and their players and attributes in the schema. GraMi will ensure that all values in your data files adhere to the value type specified or try to cast them. GraMi will also ensure that no data records enter grakn that are incomplete (missing required attributes/players).

See the [full configuration file for phone-calls here](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/processorConfig.json).
 
#### Data Configuration

The data configuration file maps each data file and its columns to your processor configurations and specifies whether a column contains single/a list of values. In addition, you can specify the size of processing batches and the number of threads (the number of cores on your machine) to be used for the migration to fine-tune the performance (where a greater number of threads up to 8 is always better while batchsize depends on the complexity of your concept (# of attributes per entity record and/or # of players per relation record)). 

A good point to start the performance optimization is to set the number of threads equal to the number of cores on your machine and the batchsize to 500 * threads (i.e.: 4 threads => batchsize = 2000).

##### Entity Data Config Entries

To get started, define an empty object in your data configuration file. Then, for each file that you would like to migrate, create a data config entry. 

For example, for the [person](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/person.csv) data file:

Excerpt from person.csv:

```CSV
first_name,last_name,phone_number,city,age,nick_name
Melli,Winchcum,+7 171 898 0853,London,55,
Celinda,Bonick,+370 351 224 5176,London,52,
Chryste,Lilywhite,+81 308 988 7153,London,66,
D'arcy,Byfford,+54 398 559 0423,London,19,D
Xylina,D'Alesco,+7 690 597 4443,Cambridge,51,
Roldan,Cometti,+263 498 495 0617,Oxford,59,Rolly;Rolli
Cob,Lafflin,+63 815 962 6097,Cambridge,56,
Olag,Heakey,+81 746 154 2598,London,45,
...
```

The corresponding data config entry would be:

```
"person": {
    "dataPath": "/your/absolute/path/to/person.csv",    // the absolute path to your data file
    "separator": ",",                                         // the separation character used in your data file (alternatives: "\t", ";", etc...)
    "processor": "person",                              // processor from processor config file
    "batchSize": 2000,                                  // batchSize to be used for this data file
    "threads": 4,                                       // # of threads to be used for this data file
    "attributes": [                                     // attribute columns present in the data file
        {
            "columnName": "first_name",                         // column name in data file
            "generator": "first-name"                           // attribute generator in processor person to be used for the column
        },
        < lines omitted >
        {
            "columnName": "phone_number",                       // column name in data file
            "generator": "phone-number"                         // attribute generator in processor person to be used for the column
        },
        < lines omitted >
        {
            "columnName": "nick_name",                          // column name in data file
            "generator": "nick-name",                           // attribute generator in processor person to be used for the column
            "listSeparator": ";"                                // separator within column separating a list of values per data record
        }
    ]
}
```

##### Relation Data Config Entries

Given the data file [call.csv](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/call.csv):

```CSV
caller_id,callee_id,started_at,duration
+54 398 559 0423,+48 195 624 2025,2018-09-16T22:24:19,122
+263 498 495 0617,+48 195 624 2025,2018-09-18T01:34:48,514
+81 308 988 7153,+33 614 339 0298,2018-09-21T20:21:17,120
+263 498 495 0617,+33 614 339 0298,2018-09-17T22:10:34,144
+54 398 559 0423,+7 552 196 4096,2018-09-25T20:24:59,556
+81 308 988 7153,+351 515 605 7915,2018-09-23T22:23:25,336
...
```

The data config entry would be:

```
"calls": {
    "dataPath": "/your/absolute/path/to/call.csv",      // the absolute path to your data file
    "separator": ",",                                         // the separation character used in your data file (alternatives: "\t", ";", etc...)
    "processor": "call",                                // processor from processor config file
    "batchSize": 100,                                   // batchSize to be used for this data file
    "threads": 4,                                       // # of threads to be used for this data file
    "players": [                                        // player columns present in the data file
        {
            "columnName": "caller_id",                      // column name in data file
        "generator": "caller"                               // player generator in processor call to be used for the column
        },
        {
            "columnName": "callee_id",                      // column name in data file
            "generator": "callee"                           // player generator in processor call to be used for the column
        }
    ],
    "attributes": [                                     // attribute columns present in the data file
        {
            "columnName": "started_at",                     // column name in data file
        "generator": "started-at"                           // attribute generator in processor call to be used for the column
        },
        {
            "columnName": "duration",                       // column name in data file
            "generator" : "duration"                        // attribute generator in processor call to be used for the column
        }
    ]
}
```

Do above for all data files that need to be migrated.

For troubleshooting, it might be worth setting the troublesome data configuration entry to a single thread, as the log messages for error from grakn are more verbose and specific that way...

See the [full configuration file for phone-calls here](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/dataConfig.json).

### Using GraMi in your Java Application:

#### Add GraMi as dependency

Maven:

Add jitpack and grakn as repositories:

```XML
<repositories>
    <repository>
        <id>grakn.ai</id>
        <url>https://repo.grakn.ai/repository/maven/</url>
    </repository>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
```

Add GraMi as dependency:
```XML
<dependency>
    <groupId>io.github.bayer-science-for-a-better-life</groupId>
    <artifactId>grami</artifactId>
    <version>0.0.1</version>
</dependency>
```

Gradle:

Add jitpack and grakn as repositories:
```
repositories {
    ...
    maven { url 'https://repo.grakn.ai/repository/maven/'}
    maven { url 'https://jitpack.io' }
}
```

Add GraMi as dependency:
```
dependencies {
    ...
    implementation 'com.github.bayer-science-for-a-better-life:grami:0.0.1'
}
```

#### Create Migration Class

In your favorite IDE, create a Class that will handle your migration (here: migration.Migration):

```Java
package migration;

import configuration.MigrationConfig;
import migrator.GraknMigrator;
import java.io.*;

public class Migration {

    private static final String schema = "/path/to/your/schema.gql";
    private static final String processorConfig = "/path/to/your/processorConfig.json";
    private static final String dataConfig = "/path/to/your/dataConfig.json";
    private static final String migrationStatus = "/path/to/your/migrationStatus.json";

    private static final String graknURI = "127.0.0.1:48555";               // defines which grakn server to migrate into
    private static final String keyspaceName = "yourFavoriteKeyspace";      // defines which keyspace to migrate into

    private static final MigrationConfig migrationConfig = new MigrationConfig(graknURI, keyspaceName, schema, dataConfig, processorConfig);

    public static void main(String[] args) throws IOException {
        GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatus, true);
        mig.migrate(true, true);
    }
}
```

The boolean flag cleanAndMigrate set to *true* as shown in:
```Java
GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatus, true);
```
will, if exists, delete the schema and all data in the given keyspace.
If set to *false*, GraMi will continue migration according to the migrationStatus file - meaning it will continue where it left off previously.


As for
```Java
mig.migrate(true, true);
```
there are four possibilities in total for the migrateEntities and migrateRelations flags, respectively:

 - (*true*, *true*) - migrate everything in dataConfig
 - (*true*, *false*) - migrate only entities (can be useful for debugging)
 - (*false*, *false*) - do not migrate data, only write schema (note: cleanAndMigrate flag as desribed above must also be *true*)
 - (*false*, *true*) - set to (*false*, *false*)

#### Configure Logging

For control of GraMi logging, add the following to your log4j2.xml configuration:

```XML
<Configuration>
    <Appenders>
        <Console name="Console">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
        <File name="logFile" fileName="grami-log.log">      <!-- change log file path here -->
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </File>
        <File name="dataLogFile" fileName="grami-data-warn.log">    <!-- change data issue log file path here -->
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </File>
        <Async name="AsyncLog">
            <AppenderRef ref="logFile"/>
        </Async>
        <Async name="AsyncData">
            <AppenderRef ref="dataLogFile"/>
        </Async>
    </Appenders>
    <Loggers>
        <Logger name="com.bayer.dt.grami.data" level="WARN">
            <AppenderRef ref="AsyncData"/>
        </Logger>
        <Logger name="com.bayer.dt.grami" level="INFO">     <!-- change level to DEBUG or TRACE for more information. Set to INFO for large imports -->
            <AppenderRef ref="Console"/>
            <AppenderRef ref="AsyncLog"/>
        </Logger>
        <Root level="OFF" />
    </Loggers>
</Configuration>
```

For tracking the progress of your importing, the suggested logging level for GraMi is "INFO". For more detailed output, set the level to DEBUG or TRACE at your convenience:

### Using GraMi as a Command-Line Application:

Download the .zip/.tar file [here](https://github.com/bayer-science-for-a-better-life/grami/releases). After unpacking, you can run it directly out of the /bin directory:

```Shell
./bin/grami \
-d /path/to/dataConfig.json \
-p /path/to/processorConfig.json \
-m /path/to/migrationStatus.json \
-s /path/to/schema.gql \
-k yourFavoriteKeyspace \
-cf
```

grami will create two log files (one for the application progress/warnings/errors, one concerned with data validity) in the grami directory for your convenience. 

## Step-by-Step Tutorial

A complete tutorial can be found [on Medium](https://medium.com/@hkuich/introducing-grami-a-data-migration-tool-for-grakn-d4051582f867).

## Compatibility

GraknMigrator is tested for 
 - [grakn-core](https://github.com/graknlabs/grakn) >= 1.8.2 
 - [client-java](https://github.com/graknlabs/client-java) >= 1.8.3

## Contributions

GraknMigrator was built @[Bayer AG](https://www.bayer.com/) in the Semantic and Knowledge Graph Technology Group with the support of the engineers @[Grakn Labs](https://github.com/orgs/graknlabs/people)

## Licensing

This repository includes software developed at [Bayer AG](https://www.bayer.com/).  It is released under a [GNU-3 General Public License](https://www.gnu.org/licenses/gpl-3.0.de.html).
 
## Credits

Icon in banner by [Smashicons](https://www.flaticon.com/de/autoren/smashicons) from [Flaticon](https://www.flaticon.com/)
