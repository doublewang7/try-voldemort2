package com.cisco.wap.exception;

public class VoldemortConfigError extends Exception {

    public VoldemortConfigError(String message) {
        super(String.format("Voldemort configuration issues, %s.", message));
    }
}
