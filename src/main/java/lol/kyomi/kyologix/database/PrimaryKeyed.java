package lol.kyomi.kyologix.database;

import org.jetbrains.annotations.NotNull;

/**
 * An interface to describe an object as having a Primary Key. This interface should typically be used
 * within a {@link Table} or {@link lol.kyomi.kyologix.cdms.DatabaseCacheManager DatabaseCacheManager} or
 * a custom implementation.
 * @param <K> The key type of the primary key, i.e. {@link java.util.UUID UUID}
 */
public interface PrimaryKeyed<K> {

	/**
	 * Gets the Primary Key associated with the object.
	 * @return the Primary Key
	 */
	@NotNull K getPrimaryKey();

}