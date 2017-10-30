# Database Migration tool for Neo4j

The **dbmigrate** plugin is a version control for neo4j!

dbmigrate is very useful when:
- Important changes done to the databases needs to be tracked, just like *git* does to our code base, so that we can easily move back and forth database states
- Changes made to database are to be shared to the entire team in a systematic way

Database migration is a very old concept and dbmigrate aims to bring the same power to neo4j. Traditionally database migration is used for strict schema database, but it is also very useful when situations like above arise. 

## Installation

Download the .jar file from the latest release (https://github.com/Nithanaroy/neo4j-db-schema-migration-tool/releases) and copy it to the `plugin` directory of your Neo4j instance just like any other APOC installation.

[Note]
This project requires a Neo4j 3.0.0 snapshot or milestone dependency.

## Usage

Having a basic understanding of the concept of database migration and neo4j APOC helps to relate the below steps better.

- Create a migration file in a folder (on your file system) of your choice using `CALL dbmigrate.createMigrationFile(abs_folder_path, new_file_name)` APOC in neo4j browser or CLI. This creates a file with a name of the format, **<number>-yourFileName.json**. Here **number** is the unique version number for this migration and I will refer to it as version from now on.
- Update the contents of the file by filling the `upgrade`, `downgrade`, `description` (optional) values. Just like in any traditional migration, write the cypher which makes any change to the database under `upgrade`. Similarly write the cypher which undoes this action under `downgrade`. `description` is like to a note to everyone what change is made to the database in this migration file.
- Run the migration file using `CALL dbmigrate.toLatest(abs_folder_path)` APOC in neo4j browser or CLI. The cypher in `upgrade` field is run and version of the database will be updated!
- Congratulations you ran your first migration!

## API

- `dbmigrate.createMigrationFile(abs_folder_path, filename)`: creates a new migration file in the folder specified. It is recommended to use this script to create a new migration file
- `dbmigrate.toLatest(abs_folder_path)`: upgrades the database to the latest version. Runs the `upgrade` cyphers from all the **newer** files in the folder specified from the current version
- `dbmigrate.toOldest(abs_folder_path)`: down grades the database to the initial version. Runs the `downgrade` cyphers from all the **older** files in the folder specified from the current version
- `dbmigrate.version()`: returns the current version of the database
- `dbmigrate.upTo(abs_folder_path, version_number)`: upgrades the database to the version_number specified. This is useful when we want to move to a specific newer version from the current version
- `dbmigrate.downTo(abs_folder_path, version_number)`: down grades the database to the version_number specified. This is useful when we want to move to a specific older version of the database from the current version while debugging for example

## Building Manually

This project uses maven, to build a jar-file with the procedure in this project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/dbmigrate-*.jar`, that has to be copied to the `plugin` directory of your Neo4j instance.


## License

MIT, see LICENSE
