package com.memgres.engine;

/**
 * Cancellation signal for the statement currently running on this thread.
 *
 * <p>Nothing preempts a running statement, so a runaway one has to notice {@code statement_timeout}
 * (or a client CancelRequest) by itself. Threading a {@link Session} reference through every
 * evaluation loop would touch most of the engine, so the signal is instead bound to the executing
 * thread for the duration of one statement and long loops poll {@link #check()}. A poll is a
 * thread-local read, cheap enough to sit on a per-row path.
 */
public final class StatementCancel {

    /** PostgreSQL's wording for a statement killed by statement_timeout. */
    public static final String TIMEOUT_MESSAGE = "canceling statement due to statement timeout";

    /** PostgreSQL's wording for a statement killed by a client cancel request. */
    public static final String REQUEST_MESSAGE = "canceling statement due to user request";

    /** The signal for one statement: written by the timeout scheduler, read by its own thread. */
    public static final class Token {
        private volatile String message;

        /** The first reason wins, so a later cancel cannot relabel a timeout that already fired. */
        public void request(String reason) {
            if (message == null) message = reason;
        }

        public String message() { return message; }

        public boolean isRequested() { return message != null; }
    }

    private static final ThreadLocal<Token> CURRENT = new ThreadLocal<Token>();

    private StatementCancel() {}

    static Token current() {
        return CURRENT.get();
    }

    /** Bind (or, with null, unbind) the token the current thread's statement is cancelled through. */
    static void bind(Token token) {
        if (token == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(token);
        }
    }

    /**
     * The error a cancelled statement reports. Used where a blocking wait is broken by the
     * interrupt that accompanies a cancel and the reason has to be recovered.
     */
    public static MemgresException canceled() {
        Token token = CURRENT.get();
        String message = token != null && token.isRequested() ? token.message() : REQUEST_MESSAGE;
        return new MemgresException(message, "57014");
    }

    /**
     * Poll for cancellation from inside a long-running loop. Returns at once in the normal case;
     * throws query_canceled (57014) once the statement's timeout has expired or the client has
     * asked for it to stop.
     */
    public static void check() {
        Token token = CURRENT.get();
        if (token == null) return;
        if (token.isRequested()) {
            throw new MemgresException(token.message(), "57014");
        }
        // A CancelRequest reaches the executing thread as an interrupt; see CancelRegistry.
        if (Thread.currentThread().isInterrupted()) {
            token.request(REQUEST_MESSAGE);
            throw new MemgresException(REQUEST_MESSAGE, "57014");
        }
    }
}
