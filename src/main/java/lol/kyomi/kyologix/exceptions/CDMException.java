package lol.kyomi.kyologix.exceptions;

/**
 * <p>
 *     The {@link CDMException} class represents an exception that occurs during the operation of a {@link lol.kyomi.kyologix.cdms.CachedDataManager CachedDataManager} instance.
 *     This exception is thrown when an error is encountered while loading, saving, or deleting data from the cache source.
 * </p>
 */
public class CDMException extends Exception {

	public CDMException(String message) {
		super(message);
	}

	public CDMException(String message, Throwable cause) { super(message, cause); }

}
