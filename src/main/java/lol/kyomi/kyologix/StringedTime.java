package lol.kyomi.kyologix;

import lol.kyomi.kyologix.exceptions.StringedTimeParseException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that represents a time in a string format. The time can be in the format of "1d2h3m4s" or any combination of
 * days, hours, minutes, and seconds. The class can convert the string time to total seconds, minutes, hours, and/or days.
 * @since 1.5.0
 * @version 1.5.4
 * @author Kyomi
 */
public class StringedTime {

	/**
	 * A pattern that matches a string time in the format of "1d2h3m4s" or any combination of days, hours, minutes, and
	 */
	private static final Pattern TIME_PATTERN = Pattern.compile("(?:(\\d+)y)?(?:(\\d+)mo)?(?:(\\d+)w)?(?:(\\d+)d)?(?:(\\d+)h)?(?:(\\d+)m)?(?:(\\d+)s)?");

	private @Nullable String stringTime;

	/**
	 * Creates a new StringedTime object with the given string time.
	 * @param stringTime The string time to create the object with
	 * @throws StringedTimeParseException If the string time is invalid
	 */
	public StringedTime(@NotNull String stringTime) throws StringedTimeParseException {
		if(!validateStringTime(stringTime))
			throw new StringedTimeParseException("Invalid time format", new IllegalArgumentException("format must be in the format of 1d2h3m4s, given " + stringTime));

		this.stringTime = stringTime;
	}

	/**
	 * Validates a string time to ensure it is in the format of "1d2h3m4s" or any combination of days, hours, minutes, and
	 * @param input The string time to validate
	 * @return True if the string time is valid, otherwise false
	 */
	private boolean validateStringTime(@Nullable String input) {
		if(input == null) return false;
		Matcher matcher = TIME_PATTERN.matcher(input);
		return matcher.matches();
	}

	/**
	 * Converts the string time to milliseconds using the {@link #toSeconds()} method and multiplying it by 1000.
	 * @return The string time in milliseconds
	 * @throws StringedTimeParseException if an error occurs while parsing the string time
	 */
	public long toMilliseconds() throws StringedTimeParseException { return toSeconds() * 1000; }

	/**
	 * Converts the string time to seconds. The string time must be in the format of "1d2h3m4s" or any combination of days,
	 * hours, minutes, and seconds. If the string time is invalid, an {@link IllegalArgumentException} will be thrown.
	 * @return The string time in seconds
	 * @throws StringedTimeParseException if an error occurs while parsing the string time
	 */
	public long toSeconds() throws StringedTimeParseException {
		if (stringTime == null) throw new StringedTimeParseException("failed to parse", new IllegalStateException("stringTime is null"));

		Matcher matcher = TIME_PATTERN.matcher(stringTime);
		if (!matcher.matches()) throw new StringedTimeParseException("failed to parse", new IllegalArgumentException("Invalid stringTime format: " + stringTime));

		int years = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 0;
		int months = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 0;
		int weeks = matcher.group(3) != null ? Integer.parseInt(matcher.group(3)) : 0;
		int days = matcher.group(4) != null ? Integer.parseInt(matcher.group(4)) : 0;
		int hours = matcher.group(5) != null ? Integer.parseInt(matcher.group(5)) : 0;
		int minutes = matcher.group(6) != null ? Integer.parseInt(matcher.group(6)) : 0;
		int seconds = matcher.group(7) != null ? Integer.parseInt(matcher.group(7)) : 0;

		// Start date for calculations
		LocalDate now = LocalDate.now();

		// Calculate years and months into total days
		LocalDate futureDate = now.plusYears(years).plusMonths(months);
		long daysFromYearsAndMonths = ChronoUnit.DAYS.between(now, futureDate);

		// Add weeks and days
		long totalDays = daysFromYearsAndMonths + (weeks * 7L) + days;

		// Conversion factors
		long secondsPerMinute = 60;
		long secondsPerHour = 60 * secondsPerMinute;
		long secondsPerDay = 24 * secondsPerHour;

		// Total seconds calculation
		return (totalDays * secondsPerDay)
			   + (hours * secondsPerHour)
			   + (minutes * secondsPerMinute)
			   + seconds;
	}

	/**
	 * Converts the string time to minutes using the {@link #toSeconds()} method and dividing it by 60.
	 * @return The string time in minutes
	 * @throws StringedTimeParseException if an error occurs while parsing the string time
	 */
	public long toMinutes() throws StringedTimeParseException { return toSeconds() / 60;}

	/**
	 * Converts the string time to hours using the {@link #toMinutes()} method and dividing it by 60.
	 * @return The string time in hours
	 * @throws StringedTimeParseException if an error occurs while parsing the string time
	 */
	public long toHours() throws StringedTimeParseException { return toMinutes() / 60; }

	/**
	 * Converts the string time to days using the {@link #toHours()} method and dividing it by 24.
	 * @return The string time in days
	 * @throws StringedTimeParseException if an error occurs while parsing the string time
	 */
	public long toDays() throws StringedTimeParseException { return toHours() / 24; }

	/**
	 * Gets the string time.
	 * @return The string time
	 */
	public @Nullable String getStringTime() { return stringTime; }

	/**
	 * Sets the string time. The string time must be in the format of "1d2h3m4s" or any combination of days, hours, minutes, and seconds.
	 * @param stringTime The string time to set
	 * @throws StringedTimeParseException if the string time is invalid
	 */
	public void setStringTime(@Nullable String stringTime) throws StringedTimeParseException {
		if(!validateStringTime(stringTime))
			throw new StringedTimeParseException("Invalid time format", new IllegalArgumentException("format must be in any combo of 1d2h3m4s (such like 1h2m3s or 1s56d2m)"));
		this.stringTime = stringTime;
	}

	@Override public final boolean equals(Object o) {
		if (!(o instanceof StringedTime that)) return false;
		return Objects.equals(getStringTime(), that.getStringTime());
	}

	@Override public int hashCode() {
		return Objects.hashCode(getStringTime());
	}

	@Override public String toString() {
		return "StringedTime{" + "stringTime='" + stringTime + '\'' + '}';
	}
}
