package example;

import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Comparator;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class DBMigrate {

	private final static String setDBVersionCypher = "MERGE (v:VERSION) SET v.version = %s RETURN v";

	@Context
	public GraphDatabaseService db;

	@Procedure(value = "example.upgrade", mode = Mode.WRITE)
	@Description("Upgrades the DB to the latest version")
	public void upgrade(@Name("folder") String folder) {
		System.out.println("Hello World from upgrade");
		try {
			long currentVersion = currentDBVersion();
			JSONParser parser = new JSONParser();
			File[] files = filesToMigrate(folder, currentVersion);

			for (File file : files) {
				System.out.println(String.format("Running migration file, '%s'", file.getName()));
				long version = getVersionFromFile(file);
				JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(file.getAbsoluteFile()));
				String cypher = (String) jsonObject.get("upgrade");
				System.out.println(String.format("Executing cypher, '%s'", cypher));
				Transaction t = db.beginTx();
				db.execute(cypher);
				db.execute(String.format(setDBVersionCypher, version));
				t.success();
				t.close();
			}
		} catch (Exception e) {
			throw new RuntimeException(e); // this type is required by neo4j
		}
	}

	/**
	 * Fetches the files to run in order
	 * 
	 * @param folder
	 *            path of the folder to look for migration files
	 * @param version
	 *            current version of database
	 * @return ordered list of files
	 */
	public File[] filesToMigrate(String folder, long version) {
		ArrayList<File> resultList = new ArrayList<>();
		File[] files = new File(folder).listFiles();
		for (File file : files) {
			if (file.isFile()) {
				long fileVersion = getVersionFromFile(file);
				if (fileVersion > version) {
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
	 * 
	 * @param file
	 *            instance of file object to get the name from
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
