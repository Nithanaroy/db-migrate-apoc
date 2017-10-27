package example;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class DBMigrate {
	@Context
	public Log log;

	@Context
	public GraphDatabaseService db;

	@Procedure("example.migrate")
	@Description("Upgrades the DB to the latest version")
	public void migrate(@Name("folder") String folder) {
		System.out.println("Hello World from migrate");
		long version = currentDBVersion();

		filesToMigrate(folder, version);
	}

	/**
	 * Fetches the files to run in order
	 * @param folder path of the folder to look for migration files 
	 * @param version current version of database
	 * @return ordered list of files
	 */
	public File[] filesToMigrate(String folder, long version) {
		ArrayList<File> resultList = new ArrayList<>();
		File[] files = new File(folder).listFiles();
		for (File file : files) {
			if (file.isFile()) {
				long fileVersion = getVersionFromFile(file);
				if (fileVersion > version) {
					System.out.println("Running file, " + file.getName());
					resultList.add(file);
				}
			}
		}
		resultList.sort(new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				long v1 = getVersionFromFile(f1);
				long v2 = getVersionFromFile(f2);
				return v1 < v2 ? -1 : v1 > v2 ? 1 : 0;
			}
		});
		File[] result = new File[resultList.size()];
		resultList.toArray(result);
		return result;
	}

	/**
	 * Extracts the version number from file name
	 * @param file instance of file object to get the name from
	 * @return version number
	 */
	private long getVersionFromFile(File file) {
		long fileVersion = Long.parseLong(file.getName().split("-")[1].split("[.]")[0]);
		return fileVersion;
	}

	@Procedure("example.createMigrationFile")
	@Description("Creates a new migration file")
	public void createMigrationFile(@Name("folder") String folder) {
		System.out.println("Hello from create migration file");
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
