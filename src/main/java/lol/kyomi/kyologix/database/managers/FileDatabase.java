package lol.kyomi.kyologix.database.managers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lol.kyomi.kyologix.database.DatabaseManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A database manager for a database file, such as an SQLite or H2 database.
 *
 * @author Kyomi
 * @since 2.0
 */
public class FileDatabase implements DatabaseManager {

	private final @NotNull String databasePath;

	private final HikariConfig config;
	private @Nullable HikariDataSource dataSource;

	public FileDatabase(@NotNull String databasePath) {
		this.databasePath = databasePath;

		this.config = new HikariConfig();
		config.setJdbcUrl("jdbc:sqlite:" + databasePath);

		config.setMaximumPoolSize(10);
		config.setMinimumIdle(2);
		config.setConnectionTimeout(30000); // 30s
		config.setIdleTimeout(180000); // 180s (3m)
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