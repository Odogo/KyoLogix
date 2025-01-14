package lol.kyomi.kyologix.database.managers;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lol.kyomi.kyologix.database.DatabaseManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * A database manager for a database within system memory, typically using H2.
 * <p><b>Note:</b> This isn't typically recommended for production settings, as this may flood memory with a shitload of bytes</p>
 *
 * @author Kyomi
 * @since 2.0
 */
public class MemoryDatabase implements DatabaseManager {

	private final @NotNull String databaseName;

	private final HikariConfig config;
	private @Nullable HikariDataSource dataSource;

	public MemoryDatabase(@NotNull String databaseName) {
		this.databaseName = databaseName;

		this.config = new HikariConfig();
		config.setJdbcUrl("jdbc:h2:mem:" + databaseName + ";");

		config.setMaximumPoolSize(10);
		config.setMinimumIdle(2);
		config.setConnectionTimeout(30000); // 30s
		config.setIdleTimeout(180000); // 180s (3m)
	}

	@Override public @NotNull HikariConfig getConfiguration() { return config; }

	public @NotNull String getDatabaseName() { return databaseName; }

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