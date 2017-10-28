package example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.stream.Stream;

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
	public Stream<Message> upgrade(@Name("folder") String folder) {
		System.out.println("Running migration, upgrade");
		try {
			long currentVersion = currentDBVersion();
			long lastKnownVersion = currentVersion; // stores the last version
													// ran
			JSONParser parser = new JSONParser();
			File[] files = filesToMigrate(folder, currentVersion);

			for (File file : files) {
				lastKnownVersion = getVersionFromFile(file);
				System.out.println(String.format("Running migration file, '%s'", file.getName()));
				JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(file.getAbsoluteFile()));
				String cypher = (String) jsonObject.get("upgrade");
				System.out.println(String.format("Executing cypher, '%s'", cypher));
				Transaction t = db.beginTx();
				db.execute(cypher);
				db.execute(String.format(setDBVersionCypher, lastKnownVersion));
				t.success();
				t.close();
			}
			return Stream.of(new Message[] { new Message(
					String.format("Upgraded DB to latest version, '%s' from '%s' by running %s migration scripts",
							lastKnownVersion, currentVersion, files.length)) });
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
				try {
					long fileVersion = getVersionFromFile(file);
					if (fileVersion > version) {
						resultList.add(file);
					}
				} catch (NumberFormatException e) {
					System.out.println(String.format(
							"'%s' does not match migration file naming convention, skipping it.", file.getName()));
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
	 * Extracts the version number from file name Example filename:
	 * /migrations/23424242424-myMigration.json where version is 23424242424
	 * 
	 * @param file
	 *            instance of file object to get the name from
	 * @return version number
	 */
	private long getVersionFromFile(File file) {
		return Long.parseLong(file.getName().split("-")[0]);
	}

	@Procedure("example.createMigrationFile")
	@Description("Creates a new migration file in the folder specified with the given migration name")
	public Stream<Message> createMigrationFile(@Name("folder") String folder, @Name("name") String name) {
		System.out.println("Running create migration file");
		try {
			String fileContent = "{\"upgrade\": \"\", \"downgrade\": \"\", \"description\": \"\" }";
			String fileName = String.format("%s-%s.json", new Timestamp(System.currentTimeMillis()).getTime(), name);
			Path filepath = Paths.get(folder, fileName).toAbsolutePath();
			createFileWithContent(filepath.toString(), fileContent);
			return Stream.of(new Message[] { new Message(String
					.format("Created a template migration file for you to update at '%s'", filepath.toString())) });
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void createFileWithContent(String filepath, String content) throws IOException {
		File file = new File(filepath);
		file.createNewFile();
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();
	}

	@Procedure(value = "example.version", mode = Mode.READ)
	@Description("Find out the current DB version")
	public Stream<Message> version() {
		return Stream.of(new Message[] { new Message(String.format("%s", currentDBVersion())) });
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

	public class Message {
		public String message;

		public Message(String message) {
			this.message = message;
		}
	}
}
