package lol.kyomi.kyologix.cdms;

import lol.kyomi.kyologix.database.DatabaseManager;
import lol.kyomi.kyologix.database.PrimaryKeyed;
import lol.kyomi.kyologix.database.Table;
import lol.kyomi.kyologix.exceptions.CDMException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 *
 * @param <K> The type of the key used to identify the data in the cache
 * @param <V> The type of the value stored in the cache
 *
 * @author Kyomi
 */
public abstract class DatabaseCacheManager<K, V extends PrimaryKeyed<K>> extends Table<K, V> {

	protected final String identifier;

	protected final ConcurrentHashMap<K, V> cache;
	protected final ConcurrentHashMap<K, Long> accessTime;

	protected final long expirationTime;
	protected final ScheduledExecutorService executorService;

	/**
	 * Creates a new {@link DatabaseCacheManager} instance with the {@link DatabaseManager},
	 * the registering class of the cache manager, the expiration unit, and the time unit describing
	 * how long the expiration unit is.
	 * @param databaseManager The DatabaseManager instance created on startup.
	 *                        <b>Note:</b> You should only have one instance of this class within your application.
	 * @param registeringClass The registering class of the manager, typically a manager of the value type.
	 *                        (i.e. KingdomManager for Kingdoms)
	 * @param expiration The expiration unit, describing the literal length of when data should get settled.
	 * @param timeUnit The expiration time unit, describing the actual scaling of the expiration unit.
	 * @throws SQLException If an SQL error occurs during creating the table, see {@link Table#Table(DatabaseManager)} for more.
	 */
	public DatabaseCacheManager(
			@NotNull DatabaseManager databaseManager, // the DatabaseManager instance
			Class<?> registeringClass, // the class registering this CacheDataManager
			long expiration, // the duration of the expiration time
			TimeUnit timeUnit // the time unit for the expiration time
	) throws SQLException {
		super(databaseManager);

		this.identifier = "DCM-" + registeringClass.getSimpleName();

		this.cache = new ConcurrentHashMap<>();
		this.accessTime = new ConcurrentHashMap<>();

		this.expirationTime = timeUnit.toMillis(expiration);
		this.executorService = Executors.newSingleThreadScheduledExecutor();

		startCacheExpiry();
	}

	// -- Cache Manager Methods -- \\

	@Override public @NotNull Optional<V> fetchByPK(@NotNull K primaryKey) throws SQLException {
		Optional<V> value = Optional.ofNullable(cache.get(primaryKey));

		if(value.isPresent()) {
			accessTime.put(primaryKey, System.currentTimeMillis());
		} else {
			value = db_fetchByPK(primaryKey);
			if(value.isEmpty()) return value;

			cache.put(primaryKey, value.get());
			accessTime.put(primaryKey, System.currentTimeMillis());
		}
		return value;
	}

	@Override public @NotNull Set<V> fetchAll() throws SQLException {

	}

	@Override public void insertEntry(@NotNull V object) throws SQLException {
		cache.put(object.getPrimaryKey(), object);
		accessTime.put(object.getPrimaryKey(), System.currentTimeMillis());
		db_insertEntry(object);
	}

	@Override public void updateEntry(@NotNull V object) throws SQLException {
		cache.put(object.getPrimaryKey(), object);
		accessTime.put(object.getPrimaryKey(), System.currentTimeMillis());
		db_updateEntry(object);
	}

	@Override public void destroyEntry(@NotNull K primaryKey) throws SQLException {
		cache.remove(primaryKey);
		accessTime.remove(primaryKey);
		db_destroyEntry(primaryKey);
	}

	public boolean settleData(@NotNull K primaryKey) throws SQLException {
		V value = cache.remove(primaryKey);
		if(value == null) return false;

		accessTime.remove(primaryKey);
		db_updateEntry(value);
		return true;
	}

	// -- Abstract Methods -- \\

	/**
	 * A database operation to fetch an entry by a primary key. This method should only be the statement executing
	 * the operation, as another method will run {@link PreparedStatement#executeQuery()} for you.
	 * @param connection A connection to the database, fetched from the {@link DatabaseManager}
	 * @param primaryKey The primary key to fetch an entry from
	 * @return The completed {@link PreparedStatement} object, ready for execution.
	 * @throws SQLException if an SQL error occurred, see {@link Connection#prepareStatement(String)} for more
	 */
	protected abstract @NotNull PreparedStatement db_fetchByPK(
			@NotNull Connection connection,
			@NotNull K primaryKey
	) throws SQLException;

	/**
	 * A database operation to fetch all entries within the table
	 * @param connection
	 * @return
	 * @throws SQLException
	 */
	protected abstract @NotNull PreparedStatement db_fetchAll(
			@NotNull Connection connection
	) throws SQLException;

	protected abstract @NotNull PreparedStatement db_insertEntry(
			@NotNull Connection connection,
			@NotNull V object
	) throws SQLException;

	protected abstract @NotNull PreparedStatement db_updateEntry(
			@NotNull Connection connection,
			@NotNull V object
	) throws SQLException;

	protected abstract @NotNull PreparedStatement db_destroyEntry(
			@NotNull Connection connection,
			@NotNull K primaryKey
	) throws SQLException;

	// -- Expiry System -- \\

	protected boolean shouldDataExpire(@NotNull K key) {
		return System.currentTimeMillis() - accessTime.get(key) > expirationTime;
	}

	private void startCacheExpiry() {
		executorService.scheduleAtFixedRate(() -> {
			try {
				for(K key : cache.keySet().stream().filter(this::shouldDataExpire).collect(Collectors.toUnmodifiableSet())) {
					settleData(key);
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}, expirationTime, expirationTime, TimeUnit.MILLISECONDS);
	}

	public void shutdown() {
		executorService.shutdown();

		try {
			for (K key : cache.keySet()) {
				settleData(key);
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}