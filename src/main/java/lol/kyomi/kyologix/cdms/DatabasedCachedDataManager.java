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
	}

	@Override public Optional<V> fetchByPK(K primaryKey) throws SQLException {
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

	@Override public Set<V> fetchAll() throws SQLException {
		return db_fetchAll();
	}

	@Override public void insertEntry(V object) throws SQLException {
		cache.put(object.getPrimaryKey(), object);
		accessTime.put(object.getPrimaryKey(), System.currentTimeMillis());
		db_insertEntry(object);
	}

	@Override public void updateEntry(V object) throws SQLException {
		cache.put(object.getPrimaryKey(), object);
		accessTime.put(object.getPrimaryKey(), System.currentTimeMillis());
		db_updateEntry(object);
	}

	@Override public void destroyEntry(K primaryKey) throws SQLException {
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

	protected abstract Optional<V> db_fetchByPK(K primaryKey) throws SQLException;

	protected abstract Set<V> db_fetchAll() throws SQLException;

	protected abstract void db_insertEntry(V object) throws SQLException;

	protected abstract void db_updateEntry(V object) throws SQLException;

	protected abstract void db_destroyEntry(K primaryKey) throws SQLException;

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