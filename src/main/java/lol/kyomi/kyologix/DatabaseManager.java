package lol.kyomi.kyologix;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * <p>
 *     The {@link DatabaseManager} class is a utility class that manages the connection to a MariaDB database using the HikariCP connection pool.
 *     This class is responsible for creating and managing the connection pool, as well as providing connections to the database.
 * </p>
 * <p>
 *     The {@link DatabaseManager} class is designed to be used as a singleton, and should be instantiated once and shared across the application.
 *     The connection pool is created when the {@link DatabaseManager#connect()} method is called, and is closed when the {@link DatabaseManager#shutdown()} method is called.
 *     Connections to the database can be obtained by calling the {@link DatabaseManager#getConnection()} method.
 * </p>
 *
 * @author Kyomi
 */
public class DatabaseManager {

	private @Nullable HikariConfig config;
	private @Nullable HikariDataSource dataSource;

	/**
	 * Creates a new {@link DatabaseManager} instance with the specified JDBC URI, username, password, and maximum pool size.
	 * You can edit the configuration of the connection pool by modifying the {@link HikariConfig} object returned by {@link DatabaseManager#getConfig()}.
	 *
	 * @param jdbcUri The JDBC URI of the database.
	 * @param username The username used to connect to the database.
	 * @param password The password used to connect to the database.
	 * @param maxPoolSize The maximum number of connections in the connection pool.
	 */
	public DatabaseManager(
			@NotNull String jdbcUri,
			@NotNull String username,
			@NotNull String password,
			int maxPoolSize
	) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(jdbcUri);
		config.setUsername(username);
		config.setPassword(password);
		config.setMaximumPoolSize(maxPoolSize);

		// Performance Tweaks
		config.setMinimumIdle(2);
		config.setIdleTimeout(60000);
		config.setConnectionTimeout(30000);

		this.config = config;
	}

	/**
	 * Creates a new {@link DatabaseManager} instance with the specified host, port, database, username, password, and maximum pool size.
	 * The JDBC URI is constructed using the specified host, port, and database.
	 * You can edit the configuration of the connection pool by modifying the {@link HikariConfig} object returned by {@link DatabaseManager#getConfig()}.
	 *
	 * @param host The host of the database.
	 * @param port The port of the database.
	 * @param database The name of the database.
	 * @param username The username used to connect to the database.
	 * @param password The password used to connect to the database.
	 * @param maxPoolSize The maximum number of connections in the connection pool.
	 */
	public DatabaseManager(
			@NotNull String host,
			int port,
			@NotNull String database,
			@NotNull String username,
			@NotNull String password,
			int maxPoolSize
	) {
		this("jdbc:mariadb://" + host + ":" + port + "/" + database, username, password, maxPoolSize);
	}

	/**
	 * Returns the {@link HikariConfig} object used to configure the connection pool.
	 * @return The {@link HikariConfig} object used to configure the connection pool.
	 */
	public @Nullable HikariConfig getConfig() { return config; }

	/**
	 * Sets the {@link HikariConfig} object used to configure the connection pool.
	 * @param config The {@link HikariConfig} object used to configure the connection pool.
	 */
	public void setConfig(@Nullable HikariConfig config) { this.config = config; }

	/**
	 * Connects to the database using the specified configuration. The connection pool is created using the configuration specified in the constructor.
	 * This method cannot be called when the {@link DatabaseManager} has already been connected, i.e., when the {@link DatabaseManager#connect()} method has already been called.
	 */
	public void connect() {
		if (config == null) {
			throw new IllegalStateException("DatabaseManager has already been connected.");
		}

		dataSource = new HikariDataSource(config);
		config = null;
	}

	/**
	 * Returns a connection to the database. This method cannot be called when the {@link DatabaseManager} has not been connected, i.e.,
	 * when the {@link DatabaseManager#connect()} method has not been called.
	 * @return A connection to the database.
	 * @throws SQLException If an error occurs while obtaining a connection to the database.
	 */
	public Connection getConnection() throws SQLException {
		if (dataSource == null) {
			throw new IllegalStateException("DatabaseManager has not been connected.");
		}

		return dataSource.getConnection();
	}

	/**
	 * Shuts down the connection pool and releases all resources associated with the {@link DatabaseManager} instance.
	 * This method cannot be called when the {@link DatabaseManager} has not been connected, i.e., when the {@link DatabaseManager#connect()} method has not been called.
	 */
	public void shutdown() {
		if (dataSource == null) {
			throw new IllegalStateException("DatabaseManager has not been connected.");
		}

		dataSource.close();
		dataSource = null;
	}
}