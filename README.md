

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
 - Entity, Relation, and Relation-with-Relations Migration:
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
 - [Schema Updating](https://github.com/bayer-science-for-a-better-life/grami#schema-updating) for non-breaking changes (i.e. add to your schema or modify concepts that do not yet contain any data)
 - [Appending Attributes](https://github.com/bayer-science-for-a-better-life/grami#attribute-appending) to existing entities/relations

After [creating your processor configuration](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/processorConfig.json) and [data configuration](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/dataConfig.json), you can use GraMi
 - as a [Command Line Application](https://github.com/bayer-science-for-a-better-life/grami/releases) - no coding - configuration required 
 - in [your own Java project](https://github.com/bayer-science-for-a-better-life/grami#using-grami-in-your-java-application) - easy API - configuration required
 
Please note that the recommended way of developing your schema is still to use your favorite code editor/IDE in combination with the grakn console.

## How it works:

To illustrate how to use GraMi, we will use a slightly extended version of the "phone-calls" example [dataset](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls) and [schema](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/schema.gql) from Grakn:

### Processor Configuration

The processor configuration file describes how you want data to be migrated according to your schema. There are two difference processor types - one for entities and one for relations. 

To get started, define the "processors" list in your processor configuration file:

```JSON
{
  "processors": [
  ]
}
```

### Data Configuration

The data configuration file maps each data file and its columns to your processor configurations and specifies whether a column contains single/a list of values. In addition, you can specify the size of processing batches and the number of threads (the number of cores on your machine) to be used for the migration to fine-tune the performance (where a greater number of threads up to 8 is always better while batchsize depends on the complexity of your concept (# of attributes per entity record and/or # of players per relation record)). 

A good point to start the performance optimization is to set the number of threads equal to the number of cores on your machine and the batchsize to 500 * threads (i.e.: 4 threads => batchsize = 2000).

### Migrating Entities

For each entity in your schema, define a processor object that specifies for each entity attribute
  - its concept name
  - its value type
  - whether it is required 
 
Please note that for each entity, at least one attribute should be required to avoid inserting empty entites into grakn. All attributes declared as keys need also be required or you will get many error messages in your logs. 

We will use the "person" entity from the phone-calls example to illustrate:
 
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

Add the following processor object in your processor configuration file:

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

GraMi will ensure that all values in your data files adhere to the value type specified or try to cast them. GraMi will also ensure that no data records enter grakn that are incomplete (missing required attributes).

Next, you need to add a data configuration entry into your data configuration file. For example, for the [person](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/person.csv) data file, which looks like this:

```CSV
first_name,last_name,phone_number,city,age,nick_name
Melli,Winchcum,+7 171 898 0853,London,55,
Celinda,Bonick,+370 351 224 5176,London,52,
Chryste,Lilywhite,+81 308 988 7153,London,66,
...
```

The corresponding data config entry would be:

```
"person": {
    "dataPath": "path/to/person.csv",                   // the absolute path to your data file
    "separator": ",",                                   // the separation character used in your data file (alternatives: "\t", ";", etc...)
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

### Migrating Relations

For each relation in your schema, define a processor object that specifies
  - each relation attribute, its value type, and whether it is required
  - each relation player of type entity, its role, identifying attribute in the data file and value type, as well as whether the player is required

We will use the call relation from the phone-calls example to illustrate. Given the schema:

 ```GraphQL
call sub relation,
    relates caller,
    relates callee,
    has started-at,
    has duration,
    plays past-call;
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

Just as in the case for entities, GraMi will ensure that all values in your data files adhere to the value type specified or try to cast them. GraMi will also ensure that no data records enter grakn that are incomplete (missing required attributes/players).

We then create a mapping of the data file [call.csv](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/call.csv) to the processor configuration:

Here is an excerpt of the data file:

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
    "dataPath": "path/to/call.csv",             // the absolute path to your data file
    "separator": ",",                                   // the separation character used in your data file (alternatives: "\t", ";", etc...)
    "processor": "call",                                // processor from processor config file
    "batchSize": 100,                                   // batchSize to be used for this data file
    "threads": 4,                                       // # of threads to be used for this data file
    "players": [                                        // player columns present in the data file
        {
            "columnName": "caller_id",                      // column name in data file
            "generator": "caller"                           // player generator in processor call to be used for the column
        },
        {
            "columnName": "callee_id",                      // column name in data file
            "generator": "callee"                           // player generator in processor call to be used for the column
        }
    ],
    "attributes": [                                     // attribute columns present in the data file
        {
            "columnName": "started_at",                     // column name in data file
            "generator": "started-at"                       // attribute generator in processor call to be used for the column
        },
        {
            "columnName": "duration",                       // column name in data file
            "generator" : "duration"                        // attribute generator in processor call to be used for the column
        }
    ]
}
```

Let's not forget about a great design pattern in Grakn (adding multiple players of the same type to a single relation). To achieve this, you can also add a listSeparator for players that are in a list in a column:

Your data might look like:

```
company_name,person_id
Unity,+62 999 888 7777###+62 999 888 7778
```

```
"contract": {
    "dataPath": "src/test/resources/phone-calls/contract.csv",
    "separator": ",",
    "processor": "contract",
    "players": [
      {
        "columnName": "company_name",
        "generator": "provider"
      },
      {
        "columnName": "person_id",
        "generator": "customer",
        "listSeparator": "###"       // like this!
      }
    ],
    "batchSize": 100,
    "threads": 4
  }
```

##### Relation-with-Relation Processors

Grakn comes with the powerful feature of using relations as players in other relations. Just remember that a relation-with-relation/s must be added AFTER the relations that will act as players in the relation have been migrated. GraMi will migrate all relation-with-relations after having migrated entities and relations - but keep this in mind as you are building your graph - relations are only inserted as expected when all its players are already present.

There are two ways to add relations into other relations:

1. Either by an identifying attribute (similar to adding an entity as described above)
2. By providing players that are used to match the relation that will be added to the relation

Be aware of unintended side-effects! Should your attribute be non-unique or your two players be part of more than one relation, you will add a relation-with-relation to each matching player relation!

For example, given the following additions to our example schema:

```GraphQL
person sub entity,
    ...,
    plays peer;

call sub relation,
    ...,
    plays past-call;

# This is the new relation-with-relation:
communication-channel sub relation,
    relates peer,
    relates past-call; # this is a call relation playing past-call
```

We define the following processor object that will allow for adding a call as a past-call either by an identifying attribute or matching via its players (caller and callee):

```
{
      "processor": "communication-channel",
      "processorType": "Relation-with-Relation",
      "schemaType": "communication-channel",
      "conceptGenerators": {
        "players": {
          "peer": {
            "playerType": "person",
            "uniquePlayerId": "phone-number",
            "idValueType": "string",
            "roleType": "peer",
            "required": true
          }
        },
        "relationPlayers": {                        // this is new!
          "past-call": {                            // past-call will be the relation in the communication-channel relation
            "playerType": "call",                   // it is of relation type "call"
            "roleType": "past-call",                // and plays the role of past-call in communication-channel
            "required": true,                       // it is required
            "matchByAttribute": {                       // we can identify a past-call via its attribute
              "started-at": {                           // this is the name of the attribute processor
                "attributeType": "started-at",              // we identify by "started-at" attribute of a call
                "valueType": "datetime"                     // which is of type datetime
              }
            },
            "matchByPlayer": {                          // we can also identify a past-call via its players
              "caller": {                                   // the name of the player processor
                "playerType": "person",                         // the player is of type person
                "uniquePlayerId": "phone-number",               // is identified by a phone number
                "idValueType": "string",                        // which is of type string
                "roleType": "caller",                           // the person is a caller in the call
                "required": true                                // and its required
              },
              "callee": {                                   // the name of the player processor
                "playerType": "person",                         // the player is of type person
                "uniquePlayerId": "phone-number",               // is identified by a phone number
                "idValueType": "string",                        // which is of type string
                "roleType": "callee",                           // the person is a callee in the call
                "required": true                                // and its required
              }
            }
          }
        }
      }
    }
```

1. This is how you can add a relation based on an identifying attribute:

Given the data file [communication-channel.csv](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/communication-channel.csv):

```CSV
peer_1,peer_2,call_started_at
+54 398 559 0423,+48 195 624 2025,2018-09-16T22:24:19
+263 498 495 0617,+33 614 339 0298,2018-09-11T22:10:34### 2018-09-12T22:10:34###2018-09-13T22:10:34 ###2018-09-14T22:10:34###2018-09-15T22:10:34###2018-09-16T22:10:34
+370 351 224 5176,+62 533 266 3426,2018-09-15T12:12:59
...
```

The data config entry would be:

```
"communication-channel": {
    "dataPath": "path/to/communication-channel.csv",      // the absolute path to your data file
    "separator": ",",                                         // the separation character used in your data file (alternatives: "\t", ";", etc...)
    "processor": "communication-channel",                     // processor from processor config file
    "batchSize": 100,                                   // batchSize to be used for this data file
    "threads": 4,                                       // # of threads to be used for this data file
    "players": [                                        // player columns present in the data file
        {
            "columnName": "peer_1",                      // column name in data file
            "generator": "peer"                          // player generator in processor call to be used for the column
        },
        {
            "columnName": "peer_2",                      // column name in data file
            "generator": "peer"                          // player generator in processor call to be used for the column
        }
    ],
    "relationPlayers": [
      {
        "columnName": "call_started_at",                // this is the column in your data file containing the "matchByAttribute"
        "generator": "past-call",                       // it will use the past-call generator in your processor config
        "matchByAttribute": "started-at",               // it will use the started-at matchByAttribute processor
        "listSeparator": "###"                          // it is a list-like column with "###" as a separator
      }
    ]
}
```

2. This is how you can add a relation based on player matching:

Given the data file [communication-channel-pm.csv](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/communication-channel-pm.csv):

```CSV
peer_1,peer_2
+81 308 988 7153,+351 515 605 7915
+7 171 898 0853,+57 629 420 5680
...
```

The data config entry would be identical to the one above, except for:

```
"communication-channel-pm": {
    "dataPath": "path/to/communication-channel-pm.csv",
    ...
    "relationPlayers": [                            // each list entry is a relationPlayer
      {
        "columnNames": ["peer_1", "peer_2"],        // two players will be used to identify the relation - these are the column names containing the attribute specified in the matchByPlayer object in the processor generator "past-call" 
        "generator": "past-call",                   // the generator in the processor configuration file to be used is "past-call"
        "matchByPlayers": ["caller", "callee"]      // the two player generators specified in the matchByPlayer object in the processor generator "past-call"
      }
    ]
}
```

For troubleshooting, it might be worth setting the troublesome data configuration entry to a single thread, as the log messages for error from grakn are more verbose and specific that way...

See the [full processor configuration file for phone-calls here](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/processorConfig.json).
See the [full data configuration file for phone-calls here](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/dataConfig.json).

### Schema Updating

When using it in your own application:
```Java
package migration;

import configuration.MigrationConfig;
import migrator.GraknMigrator;
import java.io.*;

public class Migration {

    private static final String schema = "/path/to/your/updated-schema.gql";
    private static final String graknURI = "127.0.0.1:48555";               // defines which grakn server to migrate into
    private static final String keyspaceName = "yourFavoriteKeyspace";      // defines which keyspace to migrate into

    private static final SchemaUpdateConfig suConfig = new SchemaUpdateConfig(graknURI, keyspaceName, schema);

    public static void main(String[] args) throws IOException {        
        SchemaUpdater su = new SchemaUpdater(suConfig);
        su.updateSchema();
    }
}
```

or using the CLI:

```Shell
./bin/grami schema-update\
-s /path/to/schema.gql \
-k yourFavoriteKeyspace \
```

### Attribute Appending

It is often convenient to be able to append attributes to existing entities/relations that have already been inserted.

Given [append-twitter.csv](https://github.com/bayer-science-for-a-better-life/grami/tree/master/src/test/resources/phone-calls/append-twitter.csv):

```CSV
phone_number,twitter
+7 171 898 0853,@jojo
+263 498 495 0617,@hui###@bui
+370 351 224 5176,@lalulix
+81 308 988 7153,@go34
...
```

We can match a person entity on the phone-number attribute, and then insert the twitter-username as an additional attribute.

Given the following in our schema:

```GraphQL
twitter-username sub attribute,
      value string;

person sub entity,
        ...
        has twitter-username;
```

We create an append-attribute processor, which should look very familiar:

```
{
  "processor": "append-twitter-to-person",
  "processorType": "append-attribute",      // the append-attribute processor type must be set
  "schemaType": "person",                   // concept to which the attributes will be appended
  "conceptGenerators": {
    "attributes": {
    "phone-number": {                           // the attribute generator for the attribute used for identifying the entity/relation
        "attributeType": "phone-number",
        "valueType": "string"
      },
      "twitter-username": {                     // the attribute generator for the attribute to be migrated
        "attributeType": "twitter-username",
        "valueType": "string",
        "required": true
      }
    }
  }
}
```

The data config entry would look like:

```
"append-twitter": {
    "dataPath": "src/test/resources/phone-calls/append-twitter.csv",
    "separator": ",",
    "processor": "append-twitter-to-person",
    "attributes": [
      {
        "columnName": "phone_number",
        "generator": "phone-number",
        "match": true                       // the identifying attribute must contain the match flag
      },
      {
        "columnName": "twitter",
        "generator": "twitter-username",
        "listSeparator": "###"
      }
    ],
    "batchSize": 100,
    "threads": 4
  }
```

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
        mig.migrate(true, true, true, true);
    }
}
```

The boolean flag cleanAndMigrate set to *true* as shown in:
```Java
GraknMigrator mig = new GraknMigrator(migrationConfig, migrationStatus, true);
```
will, if exists, delete the schema and all data in the given keyspace.
If set to *false*, GraMi will continue migration according to the migrationStatus file - meaning it will continue where it left off previously and leave the schema as it is.


As for
```Java
mig.migrate(true, true, true, true);
```
 - setting all to false will only reapply the schema if cleanAndMigration is set to true - otherwise it will do nothing
 - setting the first flag to true will migrate entities
 - setting the second flag to true will migrate the relations in addition
 - setting the third flag to true will migrate the relation-with-relations in addition
 - setting the fourth flag to true will migrate the append-attributes in addition
 
These flags exist because it is sometimes convenient for debugging during early development of the database model to migrate the three different classes one after the other. 

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
./bin/grami migrate \
-d /path/to/dataConfig.json \
-p /path/to/processorConfig.json \
-m /path/to/migrationStatus.json \
-s /path/to/schema.gql \
-k yourFavoriteKeyspace \
-cm
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
