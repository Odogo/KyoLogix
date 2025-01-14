package lol.kyomi.kyologix.database.managers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lol.kyomi.kyologix.database.DatabaseManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A database manager for an external database, such as a MariaDB/MySQL server.
 *
 * @author Kyomi
 * @since 1.0
 * @version 2.0
 */
public class ExternalDatabase implements DatabaseManager {

	private final HikariConfig config;
	private @Nullable HikariDataSource dataSource;

	/**
	 * Creates a new ExternalDatabase manager using any jdbcUrl given.
	 * @param jdbcUrl the jdbcUrl (note: provide the driver necessary with your url)
	 * @param username the username to authenticate with
	 * @param password the password to authenticate with username
	 */
	public ExternalDatabase(@NotNull String jdbcUrl, @NotNull String username, @NotNull String password) {
		this.config = new HikariConfig();
		config.setJdbcUrl(jdbcUrl);
		config.setUsername(username);
		config.setPassword(password);

		config.setMaximumPoolSize(10);
		config.setMinimumIdle(2);
		config.setConnectionTimeout(30000); // 30s
		config.setIdleTimeout(180000); // 180s (3m)
	}

	/**
	 * Creates a new ExternalDatabase manager using MariaDB with the given information.
	 * @param host the host address to connect with
	 * @param port the port to use alongside the address
	 * @param database the database name to access
	 * @param username the username to authenticate with
	 * @param password the password to authenticate with username
	 */
	public ExternalDatabase(@NotNull String host, int port, @NotNull String database, @NotNull String username, @NotNull String password) {
		this("jdbc:mariadb://" + host + ":" + port + "/" + database, username, password);
	}

	@Override public @NotNull HikariConfig getConfiguration() { return config; }

	@Override public void connect() {
		if(dataSource != null) throw new IllegalStateException("dataSource is already active, shutdown to connect again");
		dataSource = new HikariDataSource(config);
	}

	@Override public @NotNull Connection getConnection() throws SQLException {
		if(dataSource == null) throw new IllegalStateException("dataSource is inactive, connect to get a connection");
		return dataSource.getConnection();
	}

	@Override public void shutdown() {
		if(dataSource == null) throw new IllegalStateException("dataSource is already inactive, connect to shutdown again");

		dataSource.close();
		dataSource = null;
	}
}