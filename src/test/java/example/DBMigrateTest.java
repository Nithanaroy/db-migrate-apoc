package example;

import static org.neo4j.driver.v1.Values.parameters;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.harness.junit.Neo4jRule;

public class DBMigrateTest {
	// This rule starts a Neo4j instance for us
	@Rule
	public Neo4jRule neo4j = new Neo4jRule()
			// This is the Procedure we want to test
			.withProcedure(DBMigrate.class);

	@Test
	public void shouldFetchMigrationsToRun() {
		// In a try-block, to make sure we close the driver and session after
		// the test
		try (Driver driver = GraphDatabase.driver(neo4j.boltURI(),
				Config.build().withEncryptionLevel(Config.EncryptionLevel.NONE).toConfig());
				Session session = driver.session()) {
			// Given I've started Neo4j with the FullTextIndex procedure class
			// which my 'neo4j' rule above does.
			// And given I have a node in the database
			session.run("CREATE (p:VERSION {version: 6573467}) RETURN id(p)");
			session.run("CALL example.migrate(\"dummy\")");
			// When I use the index procedure to index a node
			// System.out.println(session.run("CALL
			// example.version()").single().get(0).asLong());

			// // Then I can search for that node with lucene query syntax
			// StatementResult result = session.run("CALL example.search('User',
			// 'name:Brook*')");
			// assertThat(result.single().get("nodeId").asLong(),
			// equalTo(nodeId));
		}
	}
}
