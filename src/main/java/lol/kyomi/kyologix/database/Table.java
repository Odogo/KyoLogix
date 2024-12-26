package lol.kyomi.kyologix.database;

import org.jetbrains.annotations.NotNull;

import java.sql.SQLException;
import java.util.Set;

/**
 * An abstract class that represents an object that is associated with a table in a database.
 * @author Kyomi
 */
public abstract class Table<K, V> {

    protected final @NotNull DatabaseManager databaseManager;

    /**
     * Create a new instance for an object that also represents a database table.
     * @param databaseManager The database manager for the application
     * @throws SQLException if an SQL error occurred during the creation of the table.
     */
    public Table(@NotNull DatabaseManager databaseManager) throws SQLException {
        this.databaseManager = databaseManager;

        createTableIfNotExists();
    }

    /**
     * An abstract method to execute an SQL statement to create the table
     * if it does not exist with the defined properties.
     * @throws SQLException if an SQL error, or related, occurred during the connection or statement execution.
     */
    protected abstract void createTableIfNotExists() throws SQLException;

    protected abstract void insertEntry(V object) throws SQLException;

    protected abstract void updateEntry(V object) throws SQLException;

    protected abstract void destroyEntry(V object) throws SQLException;

    protected abstract Set<V> fetchAll() throws SQLException;

    protected abstract V fetchByPK(K primaryKey) throws SQLException;
}