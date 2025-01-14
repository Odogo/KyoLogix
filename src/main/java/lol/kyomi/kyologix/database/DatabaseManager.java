package lol.kyomi.kyologix.database;

import com.zaxxer.hikari.HikariConfig;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * An interface to declare an object that which handles a connection to a database.
 * @author Kyomi
 * @since 1.0
 * @version 2.0
 */
public interface DatabaseManager {

	/**
	 * Gets the current configuration for this database manager
	 * @return the configuration
	 */
	@NotNull HikariConfig getConfiguration();

	/**
	 * Creates the data source object and is ready for any {@link #getConnection()} and statements
	 */
	void connect();

	/**
	 * Gets a connection from the pool to the active database
	 * @return a connection
	 * @throws SQLException if a connection could not be fetched
	 */
	@NotNull Connection getConnection() throws SQLException;

	/**
	 * Shutdown the connection pool and destroys the data source object
	 */
	void shutdown();

}