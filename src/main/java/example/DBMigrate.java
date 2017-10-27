package example;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class DBMigrate {
	@Context
	public Log log;

	@Context
	public GraphDatabaseService db;

	@Procedure("example.migrate")
	public void migrate(@Name("folder") String folder) {
		System.out.println("Hello World from sysout");

		long version = currentDBVersion();
	}

	public long currentDBVersion() {
		long currentVersion = -1; // a time stamp
		ResourceIterator<Node> nodes = db.findNodes(Label.label("VERSION"));
		if (nodes.hasNext()) {
			Node n = nodes.next();
			currentVersion = (long) n.getProperty("version");
		}
		return currentVersion;
	}
}
