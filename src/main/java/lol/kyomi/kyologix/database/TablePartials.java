package lol.kyomi.kyologix.database;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * An abstract class to be the middle-man between the application and the database. The class handles mapping and statements for all CRUD related actions.
 * <p>
 *     This class handles insert operations with use of partials. See {@link Table} for non-partial tables.
 * </p>
 *
 * @param <K> the primary key type for this table
 * @param <V> the value type (which should implement the {@link PrimaryKeyed} interface)
 * @param <PartialV> the partial data type to use. {@link V} should extend {@link PartialV}
 *
 * @since 1.0
 * @version 2.0
 */
public abstract class TablePartials<K, V extends PrimaryKeyed<K>, PartialV> {

	private final @NotNull DatabaseManager databaseManager;

	private final @NotNull String tableName;
	private final @NotNull String pkColumn;

	/**
	 * Creates a new table object for a database
	 * @param databaseManager the database manager to use and get connections with
	 * @param tableName the table's name, should be accurate
	 * @param primaryKeyName the table's primary key field, should be accurate
	 * @throws SQLException if getting the connection, preparing the statement, or executing the statement for the table creation within the database failed
	 */
	public TablePartials(@NotNull DatabaseManager databaseManager, @NotNull String tableName, @NotNull String primaryKeyName) throws SQLException {
		this.databaseManager = databaseManager;
		this.tableName = tableName;
		this.pkColumn = primaryKeyName;

		// Attempt to create the table
		try(Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = createTableStatement(connection)) {
				statement.execute();
			}
		}
	}

	/**
	 * Returns the database table's name
	 * @return the table name
	 */
	public @NotNull String getTableName() { return tableName; }

	/**
	 * Returns the primary key's column name
	 * @return the PK's column name
	 */
	public @NotNull String getPrimaryKeyColumnName() { return pkColumn; }

	/**
	 * An abstract method for creating the statement which creates the table within the database.
	 * @param connection the connection to use for making the {@link PreparedStatement} with {@link Connection#prepareStatement(String)}
	 * @return a completed {@link PreparedStatement} which will run {@link PreparedStatement#execute()} with
	 */
	protected abstract @NotNull PreparedStatement createTableStatement(@NotNull Connection connection) throws SQLException;

	/**
	 * An abstract method for inserting (or "creating") an entry within the database
	 * @param connection the connection to use for making the {@link PreparedStatement} with {@link Connection#prepareStatement(String)}
	 * @param value the value to create the statement on
	 * @return a completed {@link PreparedStatement} which will run {@link PreparedStatement#executeUpdate()} with
	 * @throws SQLException if creating the statement failed (handled externally)
	 */
	protected abstract @NotNull PreparedStatement insertEntryStatement(@NotNull Connection connection, @NotNull PartialV value) throws SQLException;

	/**
	 * An abstract method for updating an entry within the database
	 * @param connection the connection to use for making the {@link PreparedStatement} with {@link Connection#prepareStatement(String)}
	 * @param newValue the new value for an entry, use {@link PrimaryKeyed#getPrimaryKey()} to get its primary key
	 * @return a completed {@link PreparedStatement} which will run {@link PreparedStatement#executeUpdate()} with
	 * @throws SQLException if creating the statement failed (handled externally)
	 */
	protected abstract @NotNull PreparedStatement updateEntryStatement(@NotNull Connection connection, @NotNull V newValue) throws SQLException;

	/**
	 * An abstract method to handle the mapping of a {@link ResultSet} object to {@link V}
	 * @param resultSet the result set to map
	 * @return the value mapped
	 * @throws SQLException if a field from the {@link ResultSet} does not exist
	 */
	protected abstract @NotNull V mapResultSet(@NotNull ResultSet resultSet) throws SQLException;

	/**
	 * Fetches an entry by its primary key from the database.
	 * @param primaryKey the primary key to fetch by
	 * @return an optional which returns with the mapped entry if found, or empty otherwise
	 * @throws SQLException if any of the following happened:
	 * <ul>
	 *     <li>A connection could not be obtained</li>
	 *     <li>The statement could not be created or executed</li>
	 *     <li>Parsing from the table to the object failed in some way</li>
	 * </ul>
	 */
	public @NotNull Optional<V> fetchByPK(@NotNull K primaryKey) throws SQLException {
		try(Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + tableName + " WHERE " + primaryKey + " = ?")) {
				statement.setObject(1, primaryKey);
				try(ResultSet resultSet = statement.executeQuery()) {
					if(resultSet.next()) {
						return Optional.of(mapResultSet(resultSet));
					}
				}
			}
		}
		return Optional.empty();
	}

	/**
	 * Fetches all entries from the database
	 * @return a set of all entries within the database
	 * @throws SQLException if any of the following happened:
	 * <ul>
	 *     <li>A connection could not be obtained</li>
	 *     <li>The statement could not be created or executed</li>
	 *     <li>Parsing from the table to the object failed in some way</li>
	 * </ul>
	 */
	public @NotNull Set<V> fetchAll() throws SQLException {
		Set<V> results = new HashSet<>();

		try(Connection connection = databaseManager.getConnection()) {
			try(
					PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + tableName);
					ResultSet resultSet = statement.executeQuery()
			) {
				while(resultSet.next()) {
					results.add(mapResultSet(resultSet));
				}
			}
		}

		return results;
	}

	/**
	 * Insert (or "create") a new entry into the database
	 * @param value the value with all creation required fields filled out (should be a constructor for this)
	 * @return a completed object with all fields completed
	 * @throws SQLException if any of the following happened:
	 * <ul>
	 *     <li>A connection could not be obtained</li>
	 *     <li>The statement could not be created or executed</li>
	 *     <li>If no rows were affected (the entry was not inserted)</li>
	 *     <li>If no ID was obtained for the given object</li>
	 * </ul>
	 */
	public @NotNull V insert(@NotNull PartialV value) throws SQLException {
		try(Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = insertEntryStatement(connection, value)) {
				int affected = statement.executeUpdate();
				if(affected == 0) throw new SQLException("Creating value failed, no rows affected.");

				try(ResultSet generatedKeys = statement.getGeneratedKeys()) {
					if(generatedKeys.next()) {
						return fetchByPK((K) generatedKeys.getObject(1)).get();
					} else throw new SQLException("Creating value failed, no ID obtained.");
				}
			}
		}
	}

	/**
	 * Update a pre-existing entry with the new given data
	 * @param newValue the new data to update with
	 * @throws SQLException if any of the following happened:
	 * <ul>
	 *     <li>A connection could not be obtained</li>
	 *     <li>The statement could not be created or executed</li>
	 *     <li>If no rows were affected (the entry was not inserted)</li>
	 * </ul>
	 */
	public void update(@NotNull V newValue) throws SQLException {
		try(Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = updateEntryStatement(connection, newValue)) {
				int affected = statement.executeUpdate();
				if(affected == 0) throw new SQLException("Updating value failed, no rows affected.");
			}
		}
	}

	/**
	 * Deletes an entry from the database.
	 * <p><b>WARNING:</b> This action is irreversible. Any data deleted this way is not recoverable!</p>
	 * @param primaryKey the primary key to delete
	 * @throws SQLException if any of the following happened:
	 * <ul>
	 *     <li>A connection could not be obtained</li>
	 *     <li>The statement could not be created or executed</li>
	 *     <li>If no rows were affected (the entry was not inserted)</li>
	 * </ul>
	 */
	public void delete(@NotNull K primaryKey) throws SQLException {
		try(Connection connection = databaseManager.getConnection()) {
			try(PreparedStatement statement = connection.prepareStatement("DELETE FROM " + tableName + " WHERE " + pkColumn + " = ?")) {
				statement.setObject(1, primaryKey);

				int affectedRows = statement.executeUpdate();
				if(affectedRows == 0) throw new SQLException("Deleting value failed, no rows affected.");
			}
		}
	}
}
