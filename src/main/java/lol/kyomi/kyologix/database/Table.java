package lol.kyomi.kyologix.database;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Set;

/**
 * An abstract class that represents an object that is associated with a table in a database.
 * This is designed as a DAO (or Data Access Object) so subclasses should be a "manager"-like
 * class handling the translation of database to a Java object.
 * @author Kyomi
 */
public abstract class Table<K, V extends PrimaryKeyed<K>> {

    protected final @NotNull DatabaseManager databaseManager;

    protected final @NotNull String tableName;

    /**
     * Create a new instance for an object that also represents a database table.
     * @param databaseManager The database manager for the application
     * @param tableName The name of the table in the database
     * @throws SQLException if an SQL error occurred during the creation of the table.
     */
    public Table(@NotNull DatabaseManager databaseManager, @NotNull String tableName) throws SQLException {
        this.databaseManager = databaseManager;
        this.tableName = tableName;

        // Execute table creation, if it doesn't exist.
        try (Connection connection = this.databaseManager.getConnection()) {
            try (PreparedStatement statement = createTableIfNotExists(connection)) {
                statement.execute();
            }
        }
    }

    /**
     * An abstract method to execute an SQL statement to create the table
     * if it does not exist with the defined properties. Use {@link }
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    protected abstract PreparedStatement createTableIfNotExists(
            @NotNull Connection connection
    ) throws SQLException;

    /**
     * An abstract method to execute an SQL statement to fetch a specific entry by its primary key from the table.
     * @param primaryKey the primary key to get an entry from
     * @return The object associated with the primary key, or null if none exists with the key.
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    public abstract @NotNull Optional<V> fetchByPK(@NotNull K primaryKey) throws SQLException;

    /**
     * An abstract method to execute an SQL statement to fetch all entries from the table.
     * @return An unmodifiable set of entries from the database.
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    public abstract @NotNull Set<V> fetchAll() throws SQLException;

    /**
     * An abstract method to execute an SQL statement to insert a new entry into the table.
     * @param object The object to insert into this table
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    public abstract void insertEntry(@NotNull V object) throws SQLException;

    /**
     * An abstract method to execute an SQL statement to update an entry into the table.
     * @param object The object to update the table with
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    public abstract void updateEntry(@NotNull V object) throws SQLException;

    /**
     * An abstract method to execute an SQL statement to destroy an entry from the table.
     * @param primaryKey the primary key of the object to destroy.
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    public abstract void destroyEntry(@NotNull K primaryKey) throws SQLException;
}