package lol.kyomi.kyologix.database;

/**
 * Declare an object that which has a primary key associated with it.
 * <p>A "primary key" should be unique for each instance and no key should relate to two or more objects.</p>
 * @param <K> the key's type
 * @since 1.0
 * @version 2.0
 */
public interface PrimaryKeyed<K> {

	/**
	 * Gets the primary key for this object
	 * @return the primary key
	 */
	K getPrimaryKey();

}
