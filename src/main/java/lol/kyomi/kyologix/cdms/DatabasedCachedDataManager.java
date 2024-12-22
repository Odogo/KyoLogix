package lol.kyomi.kyologix.cdms;

import lol.kyomi.kyologix.DatabaseManager;
import lol.kyomi.kyologix.exceptions.CDMException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *     The {@link DatabasedCachedDataManager} class is an abstract class that extends the {@link CachedDataManager} class and provides a base
 *     implementation for a {@link CachedDataManager} that uses a database as its source.
 *     This class provides methods for creating, reading, updating, and deleting data from the database source. It also provides methods for bulk operations.
 * </p>
 * <p>
 *     The {@link DatabasedCachedDataManager} class requires a {@link DatabaseManager} instance to be passed to its constructor.
 *     This {@link DatabaseManager} instance is used to create connections to the database.
 *     The {@link DatabaseManager} class is a simple wrapper around the HikariCP connection pool library.
 *     It provides a simple API for creating connections to a database.
 * </p>
 * @see CachedDataManager
 * @param <K> The type of the key used to identify the data in the cache
 * @param <V> The type of the value stored in the cache
 *
 * @author Kyomi
 */
public abstract class DatabasedCachedDataManager<K, V> extends CachedDataManager<K, V> {

	protected final DatabaseManager databaseManager;

	/**
	 * Constructs a new {@link DatabasedCachedDataManager} instance with the specified registering class, expiration time, time unit, and database manager.
	 * @param registeringClass The class that is registering the {@link DatabasedCachedDataManager} instance
	 * @param expirationTime The expiration time for the cached data
	 * @param timeUnit The time unit for the expiration time
	 * @param databaseManager The {@link DatabaseManager} instance to use for creating connections to the database
	 */
	public DatabasedCachedDataManager(Class<?> registeringClass, long expirationTime, TimeUnit timeUnit, DatabaseManager databaseManager) throws CDMException {
		super(registeringClass, expirationTime, timeUnit);
		this.databaseManager = databaseManager;

		initialize();
	}

	@Override protected void createInSource(@NotNull K key, @NotNull V value) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareCreateStatement(connection, key, value)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to create in source", e);
		}
	}

	@Override protected @Nullable V readFromSource(@NotNull K key) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareReadStatement(connection, key)) {
				try(ResultSet resultSet = statement.executeQuery()) {
					if (resultSet.next()) {
						return deserialize(resultSet);
					}
				}
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to read from source", e);
		}
		return null;
	}

	@Override protected void updateInSource(@NotNull K key, @NotNull V newValue) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareUpdateStatement(connection, key, newValue)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to update in source", e);
		}
	}

	@Override protected void deleteFromSource(@NotNull K key) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareDeleteStatement(connection, key)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to delete from source", e);
		}
	}

	@Override protected void bulkCreateInSource(@NotNull Map<K, V> entries) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareBatchCreateStatement(connection, entries)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to bulk create in source", e);
		}
	}

	@Override protected Map<@NotNull K, @Nullable V> bulkReadFromSource(@Nullable List<K> keys) throws CDMException {
		Map<K, V> results = new ConcurrentHashMap<>();
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareBatchReadStatement(connection, keys)) {
				try(ResultSet resultSet = statement.executeQuery()) {
					while (resultSet.next()) {
						V value = deserialize(resultSet);
						results.put(null, value);
					}
				}
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to bulk read from source", e);
		}
		return results;
	}

	@Override protected void bulkUpdateInSource(@NotNull Map<K, V> entries) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareBatchUpdateStatement(connection, entries)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to bulk update in source", e);
		}
	}

	@Override protected void bulkDeleteFromSource(@NotNull List<K> keys) throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = prepareBatchDeleteStatement(connection, keys)) {
				statement.executeUpdate();
			}
		} catch (SQLException e) {
			throw new CDMException("Failed to bulk delete from source", e);
		}
	}

	/**
	 * Initialization method that is called when the {@link DatabasedCachedDataManager} instance is constructed.
	 * This method can be overridden by subclasses to perform any necessary initialization steps.
	 */
	@Override protected void initialize() throws CDMException {
		try (Connection connection = databaseManager.getConnection()) {
			createTable(connection);
		} catch (SQLException e) {
			throw new CDMException("Failed to initialize", e);
		}
	}

	/**
	 * Abstract method that must be implemented by subclasses to create the table in the database source.
	 * This method is called during the initialization of the {@link DatabasedCachedDataManager} instance.
	 * @param connection The connection to the database
	 * @throws CDMException If an error occurs while creating the table
	 */
	protected abstract void createTable(Connection connection) throws CDMException;

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for creating a new entry in the database source.
	 * This method is called when a new entry is added to the cache.
	 * @param connection The connection to the database source
	 * @param key The key of the entry to create
	 * @param value The value of the entry to create
	 * @return The prepared statement for creating the entry
	 */
	protected abstract @NotNull PreparedStatement prepareCreateStatement(Connection connection, @NotNull K key, @NotNull V value);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for reading an entry from the database source.
	 * This method is called when an entry is read from the cache.
	 * @param connection The connection to the database source
	 * @param key The key of the entry to read from the database
	 * @return The prepared statement for reading the entry
	 */
	protected abstract @NotNull PreparedStatement prepareReadStatement(Connection connection, @NotNull K key);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for updating an entry in the database source.
	 * This method is called when an entry is updated in the cache.
	 * @param connection The connection to the database source
	 * @param key The key of the entry to update in the database
	 * @param newValue The new value of the entry to update
	 * @return The prepared statement for updating the entry in the database
	 */
	protected abstract @NotNull PreparedStatement prepareUpdateStatement(Connection connection, @NotNull K key, @NotNull V newValue);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for deleting an entry from the database source.
	 * This method is called when an entry is removed from the cache.
	 * @param connection The connection to the database source
	 * @param key The key of the entry to delete from the database
	 * @return The prepared statement for deleting the entry
	 */
	protected abstract @NotNull PreparedStatement prepareDeleteStatement(Connection connection, @NotNull K key);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for creating multiple entries in the database source.
	 * This method is called when multiple entries are added to the cache.
	 * @param connection The connection to the database source
	 * @param entries The entries to create in the database
	 * @return The prepared statement for creating the entries
	 */
	protected abstract @NotNull PreparedStatement prepareBatchCreateStatement(Connection connection, @NotNull Map<K, V> entries);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for reading multiple entries from the database source.
	 * This method is called when multiple entries are read from the cache.
	 * @param connection The connection to the database source
	 * @param keys The keys of the entries to read from the database
	 * @return The prepared statement for reading the entries
	 */
	protected abstract @NotNull PreparedStatement prepareBatchReadStatement(Connection connection, @Nullable List<K> keys);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for updating multiple entries in the database source.
	 * This method is called when multiple entries are updated in the cache.
	 * @param connection The connection to the database source
	 * @param entries The entries to update in the database
	 * @return The prepared statement for updating the entries
	 */
	protected abstract @NotNull PreparedStatement prepareBatchUpdateStatement(Connection connection, @NotNull Map<K, V> entries);

	/**
	 * Abstract method that must be implemented by subclasses to prepare a statement for deleting multiple entries from the database source.
	 * This method is called when multiple entries are removed from the cache.
	 * @param connection The connection to the database source
	 * @param keys The keys of the entries to delete from the database
	 * @return The prepared statement for deleting the entries
	 */
	protected abstract @NotNull PreparedStatement prepareBatchDeleteStatement(Connection connection, @NotNull List<K> keys);

	/**
	 * Abstract method that must be implemented by subclasses to deserialize a result set into a value object.
	 * This method is called when reading an entry from the database source.
	 * @param statement The result set to deserialize into a value object
	 * @return The deserialized value object
	 * @throws SQLException If an error occurs while deserializing the result set
	 */
	protected abstract @NotNull V deserialize(@NotNull ResultSet statement) throws SQLException;
}