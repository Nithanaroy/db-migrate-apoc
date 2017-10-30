package testmigrate;

import java.nio.file.Paths;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.harness.junit.Neo4jRule;

import migrate.DBMigrate;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class DBMigrateTest {
	String dataFolder = Paths.get(System.getProperty("user.dir"), "src/test/testdata/").toAbsolutePath().toString();

	@Rule
	public Neo4jRule neo4j = new Neo4jRule().withProcedure(DBMigrate.class);

	@Test
	public void testMigrateToLatest() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			StatementResult result = session.run("CALL dbmigrate.toLatest(\"" + dataFolder + "\")");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '2' from '0', by running 2 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"super admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	public void testMigrateToOldest() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			session.run("CALL dbmigrate.toLatest(\"" + dataFolder + "\")");
			StatementResult result = session.run("CALL dbmigrate.toOldest(\"" + dataFolder + "\")");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '0' from '2', by running 2 migration scripts"));

			result = session.run("MATCH (n:PERSON) return n");
			assertThat(result.hasNext(), equalTo(false));
		}
	}

	@Test
	public void testMigrateToOne() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			StatementResult result = session.run("CALL dbmigrate.upTo(\"" + dataFolder + "\", 1)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '1' from '0', by running 1 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	public void testMigrateFromOneToTwo() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			session.run("CALL dbmigrate.upTo(\"" + dataFolder + "\", 1)");
			StatementResult result = session.run("CALL dbmigrate.upTo(\"" + dataFolder + "\", 2)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '2' from '1', by running 1 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"super admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	public void testMigrateFromTwoToOne() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			session.run("CALL dbmigrate.toLatest(\"" + dataFolder + "\")");
			StatementResult result = session.run("CALL dbmigrate.downTo(\"" + dataFolder + "\", 1)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '1' from '2', by running 1 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	public void testMigrateUptoInvalid() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			StatementResult result = session.run("CALL dbmigrate.upTo(\"" + dataFolder + "\", 1000000)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '0' from '0', by running 0 migration scripts"));
		}
	}

	@Test
	public void testMigrateDownToInvalid() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			session.run("CALL dbmigrate.toLatest(\"" + dataFolder + "\")");
			StatementResult result = session.run("CALL dbmigrate.downTo(\"" + dataFolder + "\", -1)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '2' from '2', by running 0 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"super admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	// Upgrade to a lower version from a higher version should do nothing
	public void testMigrateUptoFromHigher() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			session.run("CALL dbmigrate.toLatest(\"" + dataFolder + "\")");
			StatementResult result = session.run("CALL dbmigrate.upTo(\"" + dataFolder + "\", 1)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '2' from '2', by running 0 migration scripts"));

			result = session.run("MATCH (n:PERSON {name: \"super admin\"}) return n");
			assertThat(result.single().values().size(), equalTo(1));
		}
	}

	@Test
	// Down grade to a higher version from a lower version should do nothing
	public void testMigrateDownToFromLower() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			// assuming this works
			StatementResult result = session.run("CALL dbmigrate.downTo(\"" + dataFolder + "\", 1)");
			assertThat(result.single().get("message").asString(),
					equalTo("Changed DB to version '0' from '0', by running 0 migration scripts"));
		}
	}

	@Test
	public void complainAboutInvalidMigrationFolder() {
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {

			StatementResult result = session.run("CALL dbmigrate.toLatest(\"BOGUS_FOLDER\")");
			assertThat(result.single().get("message").asString(), equalTo("Given folder does not exist"));

			result = session.run("CALL dbmigrate.downTo(\"BOGUS_FOLDER\", 1)");
			assertThat(result.single().get("message").asString(), equalTo("Given folder does not exist"));

			result = session.run("CALL dbmigrate.upTo(\"BOGUS_FOLDER\", 2)");
			assertThat(result.single().get("message").asString(), equalTo("Given folder does not exist"));

			result = session.run("CALL dbmigrate.toOldest(\"BOGUS_FOLDER\")");
			assertThat(result.single().get("message").asString(), equalTo("Given folder does not exist"));
		}
	}
}
