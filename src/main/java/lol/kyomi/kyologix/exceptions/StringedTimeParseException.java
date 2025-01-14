package lol.kyomi.kyologix.exceptions;

/**
 * An exception that is thrown when a string time cannot be parsed.
 */
public class StringedTimeParseException extends Exception {

	public StringedTimeParseException(String message) { super(message); }

	public StringedTimeParseException(String message, Throwable cause) { super(message, cause); }

}
