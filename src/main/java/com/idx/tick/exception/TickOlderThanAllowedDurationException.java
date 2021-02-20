package com.idx.tick.exception;

import com.idx.tick.model.Tick;

public class TickOlderThanAllowedDurationException extends Exception {

    private static final String MESSAGE_FORMAT = "%s is older than allowed duration of %s milliseconds";

    public TickOlderThanAllowedDurationException(Tick tick, long allowedDurationInMs) {
        super(String.format(MESSAGE_FORMAT, tick, allowedDurationInMs));
    }
}
