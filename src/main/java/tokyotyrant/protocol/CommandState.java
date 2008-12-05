package tokyotyrant.protocol;

public enum CommandState {
	/**
	 * State indicating this operation is writing data to the server.
	 */
	WRITING,
	/**
	 * State indicating this operation is reading data from the server.
	 */
	READING,
	/**
	 * State indicating this operation is complete.
	 */
	COMPLETE
}