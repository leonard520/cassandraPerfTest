import java.math.BigInteger;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.TreeSet;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryTrace;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Field;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Table;
import com.datastax.driver.mapping.annotations.UDT;

public class CassandraTester {
	public static final String CASSANDRA_SERVER = "127.0.0.1";
	public static final String KEYSPACE = "perf_test";
	public static final String TABLENAME = "movie";
	public static final int ENTRY_COUNT = 10;
	public static final String TYPENAME = "act_info";

	private static final Map<String, PreparedStatement> cachedStatements = new HashMap<>();
	public static final Random random = new Random();

	public static class Actor{
		public int id;
		public String name;
		public int age;
		public Actor(int i, String n, int a){
			this.id = i;
			this.name = n;
			this.age = a;
		}
	}
	
	public static class Movie {
		public int id;
		public String name;
		public LocalDate date;
		public ArrayList<Actor> acts;
		public Movie(int i, String n, LocalDate d){
			this.id = i;
			this.name = n;
			this.date = d;
		}
	}
	
	@UDT(keyspace = KEYSPACE, name = TABLENAME)
	public class ActorCassandra {
		public ActorCassandra(int id, String name, int age) {
			super();
			this.id = id;
			this.name = name;
			this.age = age;
		}
		public ActorCassandra(){}
		@Field(name = "id")
		private int id;
		@Field(name = "name")
		private String name;
		@Field(name = "age")
		private int age;
		public int getId() {
			return id;
		}
		public void setId(int id) {
			this.id = id;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public int getAge() {
			return age;
		}
		public void setAge(int age) {
			this.age = age;
		}
		@Override
		public String toString() {
			return "MovieCassandra [id=" + id + ", name=" + name + ", age=" + age + "]";
		}
	}
	
//	@Table(keyspace = KEYSPACE, name = TABLENAME)
//	public static class MovieCassandra {
//		@PartitionKey
//	    @Column(name = "id")
//	    private int id;
//	    @Column(name = "name")
//	    private String name;
//	    @Column(name = "online_date")
//	    private LocalDate date;
//	    @Column(name = "act_list")
//	    private List<ActorCassandra> act;
//	    public int getId() {
//			return id;
//		}
//		public void setId(int id) {
//			this.id = id;
//		}
//		public String getName() {
//			return name;
//		}
//		public void setName(String name) {
//			this.name = name;
//		}
//		public LocalDate getDate() {
//			return date;
//		}
//		public void setDate(LocalDate date) {
//			this.date = date;
//		}
//		public MovieCassandra(
//		public MovieCassandra(int id, String name, LocalDate date, List<ActorCassandra> act) {
//			super();
//			this.id = id;
//			this.name = name;
//			this.date = date;
//			this.act = act;
//		}
//	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Cluster cluster = Cluster.builder().addContactPoints(CASSANDRA_SERVER)
				.withProtocolVersion(ProtocolVersion.NEWEST_SUPPORTED).build();

		try (Session session = cluster.connect()) {
			executeStatement(session, dropSchema());
			executeStatement(session, createSchema());
			executeStatement(session, createTypeSchema());
			executeStatement(session, createTable());
		} catch (Exception e) {
			e.printStackTrace();
		}

		long start, stop, diff;
		// CREATE DATA
		try (Session session = cluster.connect()) {
			ArrayList<String> movieNames = generateMovieNames();
			ArrayList<String> actNames = generateActNames();
			ArrayList<Integer> ages = generateAges();
			ArrayList<LocalDate> dates = generateDates();

			start = System.nanoTime();
			for(int i = 0; i < ENTRY_COUNT; i++){
				Movie m = new Movie(i + 1, movieNames.get(i), dates.get(i));
				ArrayList<Actor> act = new ArrayList<>();
				Actor a1 = new Actor(2 * i, actNames.get(2 * i), ages.get(2 * i));
				Actor a2 = new Actor(2 * i + 1, actNames.get(2 * i + 1), ages.get(2 * i + 1));
				act.add(a1);
				act.add(a2);
				m.acts = act;
				executeStatement(session, createInsertStatement(), m);
			}

			stop = System.nanoTime();
			diff = stop - start;
			System.err.println("Data created in " + diff);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// QUERY DATA
		try (Session session = cluster.connect()) {
			start = System.nanoTime();
			ResultSet rs = executeStatement(session, "SELECT * FROM " + KEYSPACE + "." + TABLENAME);
			stop = System.nanoTime();
			diff = stop - start;
			
			System.err.println("Select query returned + " + rs.all().size() + " rows");
			System.err.println(getStats(rs));
			System.err.println("Data created in " + diff);

		} catch (Exception e) {
			e.printStackTrace();
		}

		cluster.close();
		System.err.println("Done.");
	}

	private static ResultSet executeStatement(Session session, String statement, Object... params) {

		PreparedStatement st = cachedStatements.get(statement);
		if (st == null) {
			st = session.prepare(statement).enableTracing();
			cachedStatements.put(statement, st);
		}

		BoundStatement boundStatement = new BoundStatement(st);
		return session.execute(boundStatement.bind(params));
	}

	private static String getStats(ResultSet results) {
		final SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss.SSS");
		ExecutionInfo executionInfo = results.getExecutionInfo();

		StringBuilder buffer = new StringBuilder();
		QueryTrace queryTrace = executionInfo.getQueryTrace();
		long lastEvent = 0;
		for (QueryTrace.Event event : queryTrace.getEvents()) {
			long elapsed = (event.getSourceElapsedMicros() / 1000) - lastEvent;
			lastEvent = elapsed;

			buffer.append(String.format("%78s | %12s | %10s | %12s | %12s\n", event.getDescription(),
					format.format(event.getTimestamp()), event.getSource(), event.getSourceElapsedMicros(),
					Duration.of(event.getSourceElapsedMicros(), ChronoUnit.MICROS).toString()));

		}

		buffer.append(String.format("Host (queried): %s", executionInfo.getQueriedHost().toString()));

		return buffer.toString();
	}

	public static ArrayList<String> generateMovieNames() {
		ArrayList<String> names = new ArrayList<>(ENTRY_COUNT);
		for (int i = 0; i < ENTRY_COUNT; i++) {
			names.add(new BigInteger(32, random).toString(32));
		}

		System.err.println("Generated names : " + Arrays.toString(names.toArray(new String[0])));

		return names;
	}

	public static ArrayList<String> generateActNames() {
		ArrayList<String> names = new ArrayList<>(2 * ENTRY_COUNT);
		for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
			names.add(new BigInteger(16, random).toString(16));
		}

		System.err.println("Generated names : " + Arrays.toString(names.toArray(new String[0])));

		return names;
	}

	public static ArrayList<Integer> generateAges() {
		Random r = new Random();
		ArrayList<Integer> ages = new ArrayList<>(2 * ENTRY_COUNT);
		for (int i = 0; i < 2 * ENTRY_COUNT; i++) {
			ages.add(r.nextInt(80));
		}

		System.err.println("Generated ages : " + Arrays.toString(ages.toArray()));

		return ages;
	}

	public static ArrayList<LocalDate> generateDates() {
		LocalDate latest = LocalDate.of(2000, 1, 1);

		Random day = new Random();
		Random ahead = new Random();

		ArrayList<LocalDate> dateSet = new ArrayList<>();

		for (int i = 0; i < ENTRY_COUNT; i++) {
			LocalDate date = ahead.nextInt(1) == 0 ? latest.minus(day.nextInt(3650), ChronoUnit.DAYS)
					: latest.plus(day.nextInt(3650), ChronoUnit.DAYS);
			dateSet.add(date);

			System.err.println("Generated " + date.toString());
		}

		return dateSet;
	}

	public static String createTypeSchema() {
		String schema = "CREATE TYPE IF NOT EXISTS " + KEYSPACE + "." + TYPENAME + " (" + "id int," + "name ascii,"
				+ "age int" + ");";

		System.err.println("Creating schema:\n" + schema);

		return schema;
	}

	public static String createSchema() {
		String schema = "CREATE KEYSPACE IF NOT EXISTS " + KEYSPACE + " WITH replication "
				+ "= {'class':'SimpleStrategy', 'replication_factor':1};";
		System.err.println("Creating schema:\n" + schema);

		return schema;
	}
	
	public static String createTable() {
		String schema = "CREATE TABLE " + KEYSPACE + "." + TABLENAME + " (" + "id int," + "name ascii,"
				+ "online_date timestamp," + "act_list list<frozen <act_info>>," + "PRIMARY KEY (id, name)) ";

		System.err.println("Creating schema:\n" + schema);

		return schema;
	}
	
	public static String dropSchema() {
		String schema = "DROP KEYSPACE IF EXISTS " + KEYSPACE + ";";
		System.err.println("Drop schema:\n" + schema);

		return schema;
	}

	public static String createInsertStatement() {
		String stmt = "INSERT INTO " + KEYSPACE + "." + TABLENAME + 
					  " (id, name, online_date, act_list)" +
				      " VALUES " + 
					  "(?, ?, ?, ?)";

		return stmt;
	}

}
