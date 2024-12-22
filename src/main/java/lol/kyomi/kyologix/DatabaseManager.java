package lol.kyomi.kyologix;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;
import java.sql.SQLException;

public class DatabaseManager {

	private final HikariDataSource dataSource;

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

		this.dataSource = new HikariDataSource(config);
	}

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

	public Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}

	public void shutdown() {
		dataSource.close();
	}

}