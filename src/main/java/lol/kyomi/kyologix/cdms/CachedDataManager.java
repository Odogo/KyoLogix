package lol.kyomi.kyologix.cdms;

import lol.kyomi.kyologix.exceptions.CDMException;

import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * <p>The {@link CachedDataManager} class provides a structured, high-performance way to manage data caching,
 * facilitating temporary storage of frequently accessed data in memory with capabilities for persistence,
 * automatic expiration, and background data management. This abstract class is designed to be extended for specific data sources,
 * such as a database, filesystem, or API, allowing customized loading and saving methods while handling cache and data lifecycle operations.</p>
 *
 * <p>Core features of {@link CachedDataManager} include:</p>
 * <ul>
 *     <li>Automatic data expiration, removing stale entries based on a specified timeout.</li>
 *     <li>In-memory caching to optimize repeated data access and reduce costly fetch operations from external sources.</li>
 *     <li>Data persistence, enabling data saving and loading from a source that can be a file or a database as per subclass implementation.</li>
 * </ul>
 *
 * <p>This class employs an internal {@link Map} for in-memory caching and uses a scheduled executor to periodically check
 * and clear expired entries. Each instance operates autonomously, making it suitable for various cache use cases.
 * Subclasses should implement the abstract methods to define how data is retrieved, stored, and removed from the source.</p>
 *
 * @param <K> The type of keys maintained by this cache. The key uniquely identifies each cached data entry.
 * @param <V> The type of cached values associated with each key. The value represents the data cached by this manager.
 *
 * @since 1.0
 * @version 1.2
 *
 * @author <p>Kyomi - Core writer and initial documentation</p>
 *         <p>ChatGPT (4o) - Adjusted for better and detailed documentation</p>
 * @deprecated This class will be overhauled in a future version to provide a more streamlined and efficient caching system.
 * 		   While the current implementation is functional, it has a lot of room for improvement and optimization.
 * 		   The new version will be more flexible, efficient, and easier to use, with better support for various data sources.
 */
@Deprecated
public abstract class CachedDataManager<K, V> {

	// --- Static --- \\

	/**
	 * Maintains a list of all active {@link CachedDataManager} instances, allowing global cache management.
	 * Each instance is added to this list upon creation and can be accessed for centralized shutdown and maintenance.
	 */
	private static final List<CachedDataManager<?, ?>> caches = new CopyOnWriteArrayList<>();

	/** Logger for capturing runtime information, warnings, and errors related to {@link CachedDataManager} operations. */
	protected static final Logger logger = Logger.getLogger(CachedDataManager.class.getName());

	/**
	 * Provides access to an unmodifiable list of all instantiated {@link CachedDataManager} objects.
	 *
	 * @return An unmodifiable list of all created {@link CachedDataManager} instances, enabling external control and inspection.
	 */
	public static List<CachedDataManager<?, ?>> getCaches() {
		return Collections.unmodifiableList(caches);
	}

	/**
	 * Iterates over each {@link CachedDataManager} instance, invoking its {@link #shutdown()} method to
	 * release resources, save cached data, and terminate scheduled tasks for graceful shutdown.
	 */
	public static void shutdownAll() {
		caches.forEach(CachedDataManager::shutdown);
	}

	// --- Object-based --- \\
	// -- Fields -- \\

	/**
	 * A unique identifier for each {@link CachedDataManager} instance, derived from the class name of the subclass.
	 * Used for logging and distinguishing between different cache managers in multi-instance environments.
	 */
	protected final String identifier;

	/**
	 * A concurrent map serving as the primary in-memory cache, associating keys with values.
	 * Entries are added, retrieved, and removed from this map according to the cache management rules defined in this class.
	 */
	private final Map<K, V> cache;

	/**
	 * A concurrent map tracking the last access time of each cached entry, represented as a timestamp in milliseconds.
	 * Used to calculate expiration and determine when entries should be purged or refreshed.
	 */
	private final Map<K, Long> accessTime;

	/**
	 * Defines the maximum allowed duration, in milliseconds, for an entry to remain in the cache without being accessed.
	 * After this period, the entry is considered expired and is removed from the cache if not accessed.
	 */
	private final long expirationTime;

	/**
	 * A scheduled executor service responsible for periodically checking and removing expired entries.
	 * This service is started upon instantiation and operates at fixed intervals, invoking {@link #startCacheExpiry()}.
	 */
	private final ScheduledExecutorService executorService;

	/**
	 * Constructs a new {@link CachedDataManager} instance, initializing the cache, expiration time, and scheduling expiry checks.
	 * Upon creation, the cache manager begins a background task to monitor and expire stale entries.
	 *
	 * @param registeringClass The class that is creating this {@link CachedDataManager} instance, used for logging and identification.
	 * @param expirationTime   The duration after which a cache entry becomes stale, triggering removal or refresh.
	 * @param timeUnit         The time unit for the expiration time, providing flexibility (e.g., seconds, minutes, hours).
	 */
	public CachedDataManager(Class<?> registeringClass, long expirationTime, TimeUnit timeUnit) {
		this.cache = new ConcurrentHashMap<>();
		this.accessTime = new ConcurrentHashMap<>();

		this.expirationTime = timeUnit.toMillis(expirationTime);
		this.executorService = Executors.newSingleThreadScheduledExecutor();

		identifier = "CDM-" + registeringClass.getSimpleName();

		caches.add(this);
		startCacheExpiry();

		logger.info("[" + identifier + "] Created CachedDataManager with expiration time of " + expirationTime + " " + timeUnit.name());
	}

	// -- Cache Operations -- \\

	/**
	 * Checks if the cache contains an entry associated with the specified key.
	 * @param key The key to check for in the cache.
	 * @return {@code true} if the cache contains the key; {@code false} otherwise.
	 */
	public boolean cacheContainsKey(@NotNull K key) {
		return cache.containsKey(key);
	}

	/**
	 * Checks if the cache contains an entry with the specified value.
	 * @param value The value to check for in the cache.
	 * @return {@code true} if the cache contains the value; {@code false} otherwise.
	 */
	public boolean cacheContainsValue(@NotNull V value) {
		return cache.containsValue(value);
	}

	/**
	 * Settles all cached data to the source, ensuring that all data is saved and up-to-date.
	 */
	public void settleCache() {
		cache.forEach((key, value) -> {
			try {
				settleEntry(key);
			} catch (CDMException e) {
				throw new RuntimeException(e);
			}
		});
		cache.clear();
		accessTime.clear();
	}

	// -- Retroactive Operations -- \\

	/**
	 * Retrieves a value associated with the specified key from the cache, if present, or loads it from the source if not cached.
	 * If the data is loaded from the source, it is also added to the cache for future access.
	 * This method optimizes access by prioritizing the cache over external retrieval.
	 *
	 * @param key The key identifying the data to retrieve.
	 * @return The cached or loaded value, or {@code null} if no value is associated with the key.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #readEntry(K)} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	@Nullable public V fetchData(@NotNull K key) {
		try {
			return readEntry(key);
		} catch (CDMException e) {
			logger.severe("[" + identifier + "] Failed to fetch data for key: " + key + " - " + e.getMessage());
			return null;
		}
	}

	/**
	 * Stores a key-value pair in the cache and the source, ensuring data persistence and quick future access.
	 * This method is useful for inserting or updating cached data while saving it externally.
	 *
	 * @param key The key under which the data is stored in the cache and source.
	 * @param value The data to be stored in the cache and source.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #createEntry(K, V)} or {@link #updateEntry(K, V)} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	public void storeData(@NotNull K key, @NotNull V value) {
		try {
			V existing = readFromSource(key);
			if (existing != null) {
				updateEntry(key, value);
			} else {
				createEntry(key, value);
			}
		} catch (CDMException e) {
			logger.severe("[" + identifier + "] Failed to store data for key: " + key + " - " + e.getMessage());
		}
	}

	/**
	 * Saves and removes a specific entry from the cache identified by its key, transferring it to the external source.
	 * This operation is useful for persisting specific cached entries and then freeing memory by clearing the entry.
	 *
	 * @param key The key identifying the data to settle.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #settleEntry(K)} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	public void settleData(@NotNull K key) {
		try {
			settleEntry(key);
		} catch (CDMException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Persists all cached data entries to the external source and clears the in-memory cache.
	 * This operation ensures that all temporary data is saved permanently and then removed from memory.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #settleCache()} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	public void settleData() {
		settleCache();
	}

	/**
	 * Completely deletes the data associated with the specified key from both the cache and source, freeing memory and storage.
	 * <p><b>WARNING:</b> This operation is irreversible and removes all instances of the data associated with the key.</p>
	 *
	 * @param key The key identifying the data entry to be removed from cache and source.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #deleteEntry(K)} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	public void destroyData(@NotNull K key) {
		try {
			deleteEntry(key);
		} catch (CDMException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Checks if the cache contains a value for the specified key without accessing the external source.
	 * This method is efficient for verifying cached entries alone, without initiating any data loading.
	 *
	 * <p><b>Note:</b> This method does not check the source; it verifies only in-memory cache presence.</p>
	 *
	 * @param key The key to verify in the cache.
	 * @return {@code true} if the cache contains an entry for the key; {@code false} otherwise.
	 * @deprecated This method is deprecated and will be removed in a future version. Use {@link #cacheContainsKey(K)} instead.
	 */
	@Deprecated
	@ApiStatus.ScheduledForRemoval(inVersion = "1.3")
	public boolean hasData(@NotNull K key) {
		return cacheContainsKey(key);
	}

	// -- Single Entry Operations -- \\

	/**
	 * Creates a new cache entry with the specified key and value, storing it in the cache and the source.
	 * @param key The key to associate with the new cache entry.
	 * @param value The value to store in the cache.
	 * @throws CDMException If an error occurs while creating the entry in the source.
	 * @apiNote {@link #updateEntry(K, V) updateEntry} may work depending on the implementation.
	 */
	public void createEntry(@NotNull K key, @NotNull V value) throws CDMException {
		cache.put(key, value);
		accessTime.put(key, System.currentTimeMillis());

		createInSource(key, value);
		logger.fine("[" + identifier + "] Created cache entry for key: " + key);
	}

	/**
	 * Reads the cache entry associated with the specified key, returning the value if present.
	 * @param key The key identifying the cache entry to read.
	 * @return The value associated with the key if found; {@code null} if the entry is not in the cache or an error occurs.
	 * @throws CDMException If an error occurs while reading the entry from the source.
	 */
	public @Nullable V readEntry(@NotNull K key) throws CDMException {
		V value = cache.get(key);
		if(value != null) {
			accessTime.put(key, System.currentTimeMillis());
			logger.fine("[" + identifier + "] Cache hit for key: " + key);
			return value;
		}

		logger.fine("[" + identifier + "] Cache miss for key: " + key);

		value = readFromSource(key);
		if (value != null) {
			cache.put(key, value);
			accessTime.put(key, System.currentTimeMillis());
		}

		return value;
	}

	/**
	 * Updates the cache entry associated with the specified key, replacing the existing value with the new one.
	 * @param key The key identifying the cache entry to update.
	 * @param newValue The new value to store in the cache.
	 * @throws CDMException If an error occurs while updating the entry in the source.
	 * @apiNote Depending on the implementation, this method may work as a create operation if the key is not found.
	 */
	public void updateEntry(@NotNull K key, @NotNull V newValue) throws CDMException {
		cache.put(key, newValue);
		accessTime.put(key, System.currentTimeMillis());

		updateInSource(key, newValue);
		logger.fine("[" + identifier + "] Updated cache entry for key: " + key);
	}

	/**
	 * Deletes the cache entry associated with the specified key, removing it from the cache and the source.
	 * <p><b>WARNING:</b> This operation is destructive and will destroy data from the source. Data destroyed this way is not recoverable unless the source has data recovery.</p>
	 * @param key The key identifying the cache entry to delete.
	 * @throws CDMException If an error occurs while deleting the entry from the source.
	 */
	public void deleteEntry(@NotNull K key) throws CDMException {
		cache.remove(key);
		accessTime.remove(key);

		deleteFromSource(key);
		logger.fine("[" + identifier + "] Deleted cache entry for key: " + key);
	}

	/**
	 * Settles the cache entry associated with the specified key, ensuring that the data is saved to the source.
	 * @param key The key identifying the cache entry to settle.
	 * @return {@code true} if the entry was successfully settled; {@code false} if no such entry exists.
	 * @throws CDMException If an error occurs while settling the entry to the source.
	 */
	public boolean settleEntry(@NotNull K key) throws CDMException {
		V value = cache.remove(key);
		if(value == null) {
			logger.fine("[" + identifier + "] No data found for key: " + key);
			return false;
		}

		accessTime.remove(key);
		updateEntry(key, value);
		logger.fine("[" + identifier + "] Settled cache entry for key: " + key);
		return true;
	}

	// -- Bulk Operations -- \\

	/**
	 * Creates multiple new cache entries with the specified keys and values, storing them in the cache and the source.
	 * @param entries A map of keys and values to create in the cache.
	 * @throws CDMException If an error occurs while creating the entries in the source.
	 */
	public void bulkCreateEntries(@NotNull Map<K, V> entries) throws CDMException {
		cache.putAll(entries);
		entries.forEach((key, value) -> accessTime.put(key, System.currentTimeMillis()));

		bulkCreateInSource(entries);
		logger.fine("[" + identifier + "] Bulk created cache entries: " + entries.keySet());
	}

	/**
	 * Reads multiple cache entries associated with the specified keys, returning the values if present.
	 * @param keys A list of keys identifying the cache entries to read.
	 * @return A map of keys and values read from the cache, with {@code null} values for keys not found.
	 * @throws CDMException If an error occurs while reading the entries from the source.
	 */
	public Map<K, V> bulkReadEntries(@Nullable List<K> keys) throws CDMException {
		Map<@NotNull K, @Nullable V> result = bulkReadFromSource(keys);

		result.forEach((key, value) -> {
			if(value != null) {
				cache.put(key, value);
				accessTime.put(key, System.currentTimeMillis());
			}
		});

		return result;
	}

	/**
	 * Updates multiple cache entries associated with the specified keys and new values, replacing the existing values.
	 * @param entries A map of keys and new values to update in the cache.
	 * @throws CDMException If an error occurs while updating the entries in the source.
	 */
	public void bulkUpdateEntries(@NotNull Map<K, V> entries) throws CDMException {
		cache.putAll(entries);
		entries.forEach((key, value) -> accessTime.put(key, System.currentTimeMillis()));

		bulkUpdateInSource(entries);
		logger.fine("[" + identifier + "] Bulk updated cache entries: " + entries.keySet());
	}

	/**
	 * Deletes multiple cache entries associated with the specified keys, removing them from the cache and the source.
	 * @param keys A list of keys identifying the cache entries to delete.
	 * @throws CDMException If an error occurs while deleting the entries from the source.
	 */
	public void bulkDeleteEntries(@NotNull List<K> keys) throws CDMException {
		keys.forEach(key -> {
			cache.remove(key);
			accessTime.remove(key);
		});

		bulkDeleteFromSource(keys);
		logger.fine("[" + identifier + "] Bulk deleted cache entries: " + keys);
	}

	// -- Abstract Operations -- \\

	/**
	 * Abstract-ish method to handle initialization of the {@link CachedDataManager} instance, such as setting up the data source.
	 * Subclasses may choose to implement this method to perform any necessary setup operations upon instantiation, if needed.
	 *
	 * <p>
	 * 		This method does not have to be implemented by subclasses if no additional setup is required. As an example, {@link DatabasedCachedDataManager}
	 * 		uses this method to create the necessary table in the database upon initialization of the cache manager.
	 * </p>
	 *
	 * <p><b>Note:</b> This method must be called by the subclass constructor to ensure proper initialization.</p>
	 * @throws CDMException If an error occurs during initialization, such as a database connection failure or query error.
	 */
	protected void initialize() throws CDMException { }

	/**
	 * Abstract method to create a new entry into an external source, such as a file or database, associated with the specified key and value.
	 * Subclasses should implement this method to define how data is saved to the source when a new entry is created in the cache.
	 *
	 * @param key The key to associate with the new entry.
	 * @param value The value to store in the source.
	 * @throws CDMException If an error occurs while creating the entry in the source, such as a database connection failure or query error.
	 */
	protected abstract void createInSource(@NotNull K key, @NotNull V value) throws CDMException;

	/**
	 * Abstract method to read an entry from an external source, such as a file or database, associated with the specified key.
	 * Subclasses should implement this method to define how data is loaded from the source when a cache miss occurs.
	 *
	 * @param key The key identifying the entry to read from the source.
	 * @return The value associated with the key if found; {@code null} if the entry is not in the source.
	 * @throws CDMException If an error occurs while reading the entry from the source, such as a database connection failure or query error.
	 */
	protected abstract @Nullable V readFromSource(@NotNull K key) throws CDMException;

	/**
	 * Abstract method to update an entry in an external source, such as a file or database, associated with the specified key and new value.
	 * Subclasses should implement this method to define how data is updated in the source when an entry is updated in the cache.
	 *
	 * @param key The key identifying the entry to update in the source.
	 * @param newValue The new value to store in the source.
	 * @throws CDMException If an error occurs while updating the entry in the source, such as a database connection failure or query error.
	 */
	protected abstract void updateInSource(@NotNull K key, @NotNull V newValue) throws CDMException;

	// -- Bulk Abstract Operations -- \\

	/**
	 * Abstract method to delete an entry from an external source, such as a file or database, associated with the specified key.
	 * Subclasses should implement this method to define how data is removed from the source when an entry is deleted from the cache.
	 *
	 * @param key The key identifying the entry to delete from the source.
	 * @throws CDMException If an error occurs while deleting the entry from the source, such as a database connection failure or query error.
	 */
	protected abstract void deleteFromSource(@NotNull K key) throws CDMException;

	/**
	 * Abstract method to bulk create multiple entries into an external source, such as a file or database, associated with the specified keys and values.
	 * Subclasses should implement this method to define how multiple entries are saved to the source when a bulk create operation is performed.
	 *
	 * <p>
	 *     This method is useful for optimizing performance when creating multiple entries at once, reducing the number of source interactions.
	 *     Implementations should handle the bulk creation of entries efficiently to minimize latency and improve throughput.
	 * </p>
	 * @param entries A map of keys and values to create in the source.
	 * @throws CDMException If an error occurs while creating the entries in the source, such as a database connection failure or query error.
	 */
	protected abstract void bulkCreateInSource(@NotNull Map<K, V> entries) throws CDMException;

	/**
	 * Abstract method to bulk read multiple entries from an external source, such as a file or database, associated with the specified keys.
	 * Subclasses should implement this method to define how multiple entries are loaded from the source when a bulk read operation is performed.
	 *
	 * <p>
	 *     This method is useful for optimizing performance when reading multiple entries at once, reducing the number of source interactions.
	 *     Implementations should handle the bulk reading of entries efficiently to minimize latency and improve throughput.
	 *     The returned list should contain values corresponding to the input keys, with {@code null} values for keys not found in the source.
	 * </p>
	 * <p>
	 *     For a {@code null} keys list, the implementation should return all entries from the source.
	 *     This is useful for initializing the cache with all available data or fetching all entries when needed (though not recommended for large datasets).
	 * </p>
	 *
	 * @param keys A list of keys identifying the entries to read from the source, or {@code null} to read all entries.
	 * @return A map of keys and values read from the source, with {@code null} values for keys not found.
	 * @throws CDMException If an error occurs while reading the entries from the source, such as a database connection failure or query error.
	 */
	protected abstract Map<@NotNull K, @Nullable V> bulkReadFromSource(@Nullable List<K> keys) throws CDMException;

	/**
	 * Abstract method to bulk update multiple entries in an external source, such as a file or database, associated with the specified keys and new values.
	 * Subclasses should implement this method to define how multiple entries are updated in the source when a bulk update operation is performed.
	 *
	 * <p>
	 *     This method is useful for optimizing performance when updating multiple entries at once, reducing the number of source interactions.
	 *     Implementations should handle the bulk updating of entries efficiently to minimize latency and improve throughput.
	 * </p>
	 * <p>
	 *     The implementation should update the entries in the source based on the provided keys and new values, ensuring that the source data is consistent with the cache.
	 *     Any other entries not defined within the input map should remain unchanged in the source, preserving data integrity.
	 * </p>
	 * <p>
	 *     If an entry does not exist in the source, it may be created based on the provided key and value, if the implementation allows. (database implementations may not allow this)
	 * 	   The method should handle the bulk update operation in a transactional manner to ensure data consistency and atomicity.
	 * </p>
	 *
	 * @param entries A map of keys and new values to update in the source.
	 * @throws CDMException If an error occurs while updating the entries in the source, such as a database connection failure or query error.
	 */
	protected abstract void bulkUpdateInSource(@NotNull Map<K, V> entries) throws CDMException;

	/**
	 * Abstract method to bulk delete multiple entries from an external source, such as a file or database, associated with the specified keys.
	 * Subclasses should implement this method to define how multiple entries are removed from the source when a bulk delete operation is performed.
	 *
	 * <p>
	 *     This method is useful for optimizing performance when deleting multiple entries at once, reducing the number of source interactions.
	 *     Implementations should handle the bulk deletion of entries efficiently to minimize latency and improve throughput.
	 * </p>
	 * <p>
	 *     The implementation should delete the entries from the source based on the provided keys, ensuring that the source data is consistent with the cache.
	 *     Any other entries not defined within the input list should remain unchanged in the source, preserving data integrity.
	 * </p>
	 * <p>
	 *     The method should handle the bulk delete operation in a transactional manner to ensure data consistency and atomicity.
	 * 	   If an entry does not exist in the source, it should be ignored, and the operation should proceed without errors.
	 * </p>
	 * @param keys A list of keys identifying the entries to delete from the source.
	 * @throws CDMException If an error occurs while deleting the entries from the source, such as a database connection failure or query error.
	 */
	protected abstract void bulkDeleteFromSource(@NotNull List<K> keys) throws CDMException;

	// -- Expiry Management -- \\

	/**
	 * Checks if the cached entry associated with the specified key has expired, based on the current time and the last access time.
	 * Expired entries are removed from the cache to maintain memory efficiency.
	 *
	 * @param key The key identifying the entry to check for expiration.
	 * @return {@code true} if the entry has expired; {@code false} otherwise.
	 */
	protected boolean shouldExpire(@NotNull K key) {
		return System.currentTimeMillis() - accessTime.get(key) > expirationTime;
	}

	/**
	 * Initiates the scheduled task to monitor and remove expired entries, ensuring the cache remains up-to-date and efficient.
	 * This method is automatically called upon instantiation, setting up the background task for cache management.
	 * <p><b>Note:</b> This method should not be called directly outside of constructor initialization.</p>
	 */
	private void startCacheExpiry() {
		executorService.scheduleAtFixedRate(() -> {
			try {
				cache.keySet().stream()
						.filter(this::shouldExpire)
						.forEach(key -> {
							try {
								settleEntry(key);
								logger.fine("[" + identifier + "] Cache entry expired and settled for key: " + key);
							} catch (CDMException e) {
								logger.log(Level.SEVERE, "[" + identifier + "] Failed to settle expired cache entry for key: " + key, e);
							}
						});
			} catch (Exception e) {
				logger.log(Level.SEVERE, "[" + identifier + "] Cache expiry task failed", e);
			}
		}, expirationTime, expirationTime, TimeUnit.MILLISECONDS);
		logger.info("[" + identifier + "] Cache expiry service started.");
	}

	/**
	 * Terminates the executor service, settles all cached data to the source, and frees resources used by this {@link CachedDataManager}.
	 * This method should be called during application or plugin shutdown to prevent resource leaks.
	 */
	public void shutdown() {
		executorService.shutdown();
		settleCache();
	}
}