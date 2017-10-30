package example;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
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
import org.json.simple.parser.ParseException;
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
	private final static String upgradeDowngradeRespTemplate = "Changed DB to version '%s' from '%s', by running %s migration scripts";
	private final static int minDBVersion = 0;

	@Context
	public GraphDatabaseService db;

	@Procedure(value = "example.toLatest", mode = Mode.WRITE)
	@Description("Upgrades the DB to the latest version")
	public Stream<Message> upgrade(@Name("folder") String folder) {
		System.out.println("Upgrading DB to the latest version");
		try {
			Object[] runStats = upgradeOrDowngradeUptoToVersion(folder, Double.POSITIVE_INFINITY,
					MigrationType.UPGRADE);
			return Stream.of(new Message[] {
					new Message(String.format(upgradeDowngradeRespTemplate, runStats[0], runStats[1], runStats[2])) });
		} catch (Exception e) {
			throw new RuntimeException(e); // this type is required by neo4j
		}
	}

	@Procedure(value = "example.upTo", mode = Mode.WRITE)
	@Description("Upgrades the DB to a version not greater than the specified version")
	public Stream<Message> upgradeTo(@Name("folder") String folder, @Name("version") long version) {
		System.out.println(String.format("Running migration, upgrade upto %s", version));
		try {
			Object[] runStats = upgradeOrDowngradeUptoToVersion(folder, version, MigrationType.UPGRADE);
			return Stream.of(new Message[] {
					new Message(String.format(upgradeDowngradeRespTemplate, runStats[0], runStats[1], runStats[2])) });
		} catch (Exception e) {
			throw new RuntimeException(e); // this type is required by neo4j
		}
	}

	@Procedure(value = "example.toOldest", mode = Mode.WRITE)
	@Description("Downgrades the DB to the oldest version")
	public Stream<Message> downgrade(@Name("folder") String folder) {
		System.out.println("Downgrading the DB the oldest version");
		try {
			Object[] runStats = upgradeOrDowngradeUptoToVersion(folder, Double.NEGATIVE_INFINITY,
					MigrationType.DOWNGRADE);
			db.execute(String.format(setDBVersionCypher, minDBVersion));
			return Stream.of(new Message[] {
					new Message(String.format(upgradeDowngradeRespTemplate, minDBVersion, runStats[1], runStats[2])) });
		} catch (Exception e) {
			throw new RuntimeException(e); // this type is required by neo4j
		}
	}

	@Procedure(value = "example.downTo", mode = Mode.WRITE)
	@Description("Downgrades the DB to a version not lesser than the specified version")
	public Stream<Message> downgradeTo(@Name("folder") String folder, @Name("version") long version) {
		System.out.println(String.format("Running migration, downgrade upto %s", version));
		try {
			Object[] runStats = upgradeOrDowngradeUptoToVersion(folder, version, MigrationType.DOWNGRADE);
			Object destinationVersion = ((int) runStats[2]) > 0 ? version : runStats[1];
			return Stream.of(new Message[] { new Message(
					String.format(upgradeDowngradeRespTemplate, destinationVersion, runStats[1], runStats[2])) });
		} catch (Exception e) {
			throw new RuntimeException(e); // this type is required by neo4j
		}
	}

	@Procedure("example.createMigrationFile")
	@Description("Creates a new migration file in the folder specified with the given migration name")
	public Stream<Message> createMigrationFile(@Name("folder") String folder, @Name("name") String name) {
		System.out.println("Running create migration file");
		try {
			String fileContent = "{\"upgrade\": \"\", \"downgrade\": \"\", \"description\": \"Write your cypher for changing the database as a value for upgrade. Write a cypher that undoes whatever upgrade does to the database as a value for downgrade. Give a small description what this migration script does in this field\" }";
			String fileName = String.format("%s-%s.json", new Timestamp(System.currentTimeMillis()).getTime(), name);
			Path filepath = Paths.get(folder, fileName).toAbsolutePath();
			createFileWithContent(filepath.toString(), fileContent);
			return Stream.of(new Message[] { new Message(String
					.format("Created a template migration file for you to update at '%s'", filepath.toString())) });
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Procedure(value = "example.version", mode = Mode.READ)
	@Description("Find out the current DB version")
	public Stream<Message> version() {
		return Stream.of(new Message[] { new Message(String.format("%s", currentDBVersion())) });
	}

	private Object[] upgradeOrDowngradeUptoToVersion(String folder, double limitingVersion, MigrationType m)
			throws IOException, ParseException, FileNotFoundException {
		long fromVersion = currentDBVersion();
		long versionIterator = fromVersion;
		long toVersion = fromVersion;
		int migrationsRan = 0;
		JSONParser parser = new JSONParser();
		File[] files = m == MigrationType.UPGRADE ? fetchOlderOrNewerFilesToVersion(folder, fromVersion, "newer")
				: fetchOlderOrNewerFilesToVersion(folder, fromVersion, "older");

		// Check if the version number exists in the list of files
		if (limitingVersion > Double.NEGATIVE_INFINITY && limitingVersion < Double.POSITIVE_INFINITY
				&& !versionFileExists(files, limitingVersion)) {
			return new Object[] { toVersion, fromVersion, migrationsRan };
		}

		for (File file : files) {
			versionIterator = getVersionFromFile(file);
			if (m == MigrationType.UPGRADE && versionIterator <= limitingVersion) {
				runMigrationFile(versionIterator, parser, file, m);
				toVersion = versionIterator;
			} else if (m == MigrationType.DOWNGRADE && versionIterator > limitingVersion) {
				runMigrationFile(versionIterator, parser, file, m);
				toVersion = versionIterator;
			} else {
				break;
			}
			migrationsRan++;
		}
		return new Object[] { toVersion, fromVersion, migrationsRan };
	}

	private void runMigrationFile(long lastKnownVersion, JSONParser parser, File file, MigrationType m)
			throws IOException, ParseException, FileNotFoundException {
		System.out.println(String.format("Running migration file, '%s'", file.getName()));
		JSONObject jsonObject = (JSONObject) parser.parse(new FileReader(file.getAbsoluteFile()));
		String cypher = (String) jsonObject.get(m == MigrationType.UPGRADE ? "upgrade" : "downgrade");
		System.out.println(String.format("Executing cypher, '%s'", cypher));
		Transaction t = db.beginTx();
		db.execute(cypher);
		db.execute(String.format(setDBVersionCypher, lastKnownVersion));
		t.success();
		t.close();
	}

	/**
	 * Fetches the migration files newer or older than given version in the
	 * order in which they should be run
	 * 
	 * @param folder
	 *            path of the folder to look for migration files
	 * @param version
	 *            base value for version beyond which newer or older values are
	 *            filtered
	 * @return list of files in the order they should be run
	 */
	private File[] fetchOlderOrNewerFilesToVersion(String folder, long version, String newerOrOlder) {
		ArrayList<File> resultList = new ArrayList<>();
		File[] files = new File(folder).listFiles();
		for (File file : files) {
			if (file.isFile()) {
				try {
					long fileVersion = getVersionFromFile(file);
					if (newerOrOlder.equals("newer") && version < fileVersion) {
						resultList.add(file);
					} else if (newerOrOlder.equals("older") && fileVersion <= version) {
						resultList.add(file);
					}
				} catch (NumberFormatException e) {
					System.out.println(String.format(
							"'%s' does not match migration file naming convention, skipping it.", file.getName()));
				}
			}
		}
		Comparator<File> ascendingComparator = new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				long v1 = getVersionFromFile(f1);
				long v2 = getVersionFromFile(f2);
				return v1 < v2 ? -1 : v1 > v2 ? 1 : 0;
			}
		};
		Comparator<File> descendingComparator = new Comparator<File>() {
			@Override
			public int compare(File f1, File f2) {
				long v1 = getVersionFromFile(f1);
				long v2 = getVersionFromFile(f2);
				return v1 < v2 ? 1 : v1 > v2 ? -1 : 0;
			}
		};
		if (newerOrOlder.equals("newer")) {
			resultList.sort(ascendingComparator);
		} else if (newerOrOlder.equals("older")) {
			resultList.sort(descendingComparator);
		}
		File[] result = new File[resultList.size()];
		resultList.toArray(result);
		return result;
	}

	private boolean versionFileExists(File[] files, double requiredVersion) {
		for (File file : files) {
			if (file.isFile()) {
				try {
					if (requiredVersion == getVersionFromFile(file))
						return true;
				} catch (NumberFormatException e) {

				}
			}
		}
		return false;
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

	private void createFileWithContent(String filepath, String content) throws IOException {
		File file = new File(filepath);
		file.createNewFile();
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(content);
		bw.close();
	}

	private long currentDBVersion() {
		long currentVersion = minDBVersion; // a time stamp
		ResourceIterator<Node> nodes = db.findNodes(Label.label("VERSION"));
		if (nodes.hasNext()) {
			Node n = nodes.next();
			currentVersion = (long) n.getProperty("version");
		}
		return currentVersion;
	}

	private enum MigrationType {
		UPGRADE, DOWNGRADE
	}

	public class Message {
		public String message;

		public Message(String message) {
			this.message = message;
		}
	}
}
