package com.leo.functions;

public class SupabaseException extends Exception {
    public SupabaseException() {
        super();
    }

    public SupabaseException(String message) {
        super(message);
    }

    public SupabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    public SupabaseException(Throwable cause) {
        super(cause);
    }

    protected SupabaseException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
