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

public abstract class DatabasedCachedDataManager<K, V> extends CachedDataManager<K, V> {

	protected final DatabaseManager databaseManager;

	public DatabasedCachedDataManager(Class<?> registeringClass, long expirationTime, TimeUnit timeUnit, DatabaseManager databaseManager) {
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

	protected abstract @NotNull PreparedStatement prepareCreateStatement(Connection connection, @NotNull K key, @NotNull V value);

	protected abstract @NotNull PreparedStatement prepareReadStatement(Connection connection, @NotNull K key);

	protected abstract @NotNull PreparedStatement prepareUpdateStatement(Connection connection, @NotNull K key, @NotNull V newValue);

	protected abstract @NotNull PreparedStatement prepareDeleteStatement(Connection connection, @NotNull K key);

	protected abstract @NotNull PreparedStatement prepareBatchCreateStatement(Connection connection, @NotNull Map<K, V> entries);

	protected abstract @NotNull PreparedStatement prepareBatchReadStatement(Connection connection, @Nullable List<K> keys);

	protected abstract @NotNull PreparedStatement prepareBatchUpdateStatement(Connection connection, @NotNull Map<K, V> entries);

	protected abstract @NotNull PreparedStatement prepareBatchDeleteStatement(Connection connection, @NotNull List<K> keys);

	protected abstract @NotNull V deserialize(@NotNull ResultSet statement) throws SQLException;
}