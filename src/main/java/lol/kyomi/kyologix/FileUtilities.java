package lol.kyomi.kyologix;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.*;

/**
 * Some file utilities to read and write files. Cause why not lol
 * @author Kyomi
 */
public class FileUtilities {

	/**
	 * Reads the full contents of a file from the file system, returning its content as a string.
	 * This static utility method is designed to facilitate reading data directly, making it accessible for
	 * subclasses or external callers without additional file-handling logic.
	 *
	 * @param file The file to read from; must be a regular file and not a directory.
	 * @return The content of the file as a {@code String}, or {@code null} if the file is empty or does not exist.
	 * @throws IllegalArgumentException if the specified {@code file} represents a directory.
	 */
	@Nullable public static String readContents(@NotNull File file) {
		if(!file.exists()) return null;
		if(file.isDirectory()) throw new IllegalArgumentException("file is a directory.");

		try(BufferedReader reader = new BufferedReader(new FileReader(file))) {
			StringBuilder builder = new StringBuilder();

			String line;
			while((line = reader.readLine()) != null)
				builder.append(line).append(System.lineSeparator());

			if(builder.toString().isEmpty() || builder.toString().isBlank()) return null;
			return builder.toString();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Writes a string to the specified file, creating the file and necessary directories if they do not exist.
	 * This method provides a straightforward way to save data in text format to the filesystem.
	 *
	 * @param file the target file to write to; if it does not exist, it will be created along with any required directories.
	 * @param contents The content to write to the file as a string.
	 * @throws IllegalArgumentException if the specified {@code file} is a directory instead of a regular file.
	 * @throws IOException if an I/O error occurs during file creation or writing, such as insufficient permissions or
	 *         disk space issues.
	 */
	public static void writeContents(@NotNull File file, @NotNull String contents) throws IOException {
		if(file.isDirectory()) throw new IllegalArgumentException("file is a directory");

		if(!file.exists()) {
			file.getParentFile().mkdirs();
			if(!file.createNewFile())
				throw new IOException("failed to create the file");
		}

		try(BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write(contents);
		}
	}
}