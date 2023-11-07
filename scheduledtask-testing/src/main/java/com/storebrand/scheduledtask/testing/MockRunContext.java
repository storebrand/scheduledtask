/*
 * Copyright 2022 Storebrand ASA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.storebrand.scheduledtask.testing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import com.storebrand.scheduledtask.ScheduledTask;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleRunContext;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.ScheduleStatus;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;

/**
 * Mock implementation of {@link ScheduleRunContext} used by {@link ScheduledTaskTestRunner} to run scheduled task
 * methods without requiring a fully wired {@link com.storebrand.scheduledtask.ScheduledTaskRegistry}.
 *
 * @author Kristian Hiim
 */
class MockRunContext implements ScheduleRunContext {

    static final String SCHEDULE_STATUS_VALID_RESPONSE_CLASS =
            "com.storebrand.scheduledtask.ScheduledTaskRunner$ScheduleStatusValidResponse";

    private static final AtomicLong __nextId = new AtomicLong();

    private final long _id;
    private final LocalDateTime _runStarted;
    private final List<LogEntry> _logEntries = new ArrayList<>();

    private volatile State _state;
    private volatile String _statusMessage = "";
    private volatile String _statusStackTrace = null;
    private volatile LocalDateTime _statusTime = null;

    MockRunContext() {
        _id = __nextId.incrementAndGet();
        _state = State.STARTED;
        _runStarted = LocalDateTime.now();
    }

    @Override
    public long getRunId() {
        return _id;
    }

    @Override
    public String getScheduledName() {
        return "TEST_SCHEDULE";
    }

    @Override
    public String getHostname() {
        return "TEST_HOST";
    }

    @Override
    public ScheduledTask getSchedule() {
        throw new IllegalStateException("Schedule is not available when running method directly using"
                + " ScheduleTaskTestRunner.runScheduledTaskMethod(method)");
    }

    @Override
    public Instant getPreviousRun() {
        return Instant.EPOCH;
    }

    @Override
    public State getStatus() {
        return _state;
    }

    @Override
    public String getStatusMsg() {
        return _statusMessage;
    }

    @Override
    public String getStatusStackTrace() {
        return _statusStackTrace;
    }

    @Override
    public LocalDateTime getRunStarted() {
        return _runStarted;
    }

    @Override
    public LocalDateTime getStatusTime() {
        return _statusTime;
    }

    @Override
    public List<LogEntry> getLogEntries() {
        synchronized (_logEntries) {
            return new ArrayList<>(_logEntries);
        }
    }

    @Override
    public void log(String msg) {
        addLogEntry(msg, null);
    }

    @Override
    public void log(String msg, Throwable throwable) {
        addLogEntry(msg, throwable);
    }

    @Override
    public ScheduleStatus done(String msg) {
        _state = State.DONE;
        _statusMessage = msg;
        _statusTime = LocalDateTime.now();
        log("[" + State.DONE + "] " + msg);
        return createValidStatus();
    }

    @Override
    public ScheduleStatus failed(String msg) {
        _state = State.FAILED;
        _statusMessage = msg;
        _statusTime = LocalDateTime.now();
        log("[" + State.FAILED + "] " + msg);
        return createValidStatus();
    }

    @Override
    public ScheduleStatus failed(String msg, Throwable throwable) {
        _state = State.FAILED;
        _statusMessage = msg;
        _statusStackTrace = throwableToStackTraceString(throwable);
        _statusTime = LocalDateTime.now();
        log("[" + State.FAILED + "] " + msg, throwable);
        return createValidStatus();
    }

    @Override
    public ScheduleStatus dispatched(String msg) {
        _state = State.DISPATCHED;
        _statusMessage = msg;
        return createValidStatus();
    }


    @Override
    public ScheduleStatus noop(String msg) {
        _state = State.NOOP;
        _statusMessage = msg;
        return createValidStatus();
    }

    private void addLogEntry(String message, Throwable throwable) {
        synchronized (_logEntries) {
            LogEntry entry = new MockLogEntry(_logEntries.size() + 1, _id, message,
                    throwableToStackTraceString(throwable), LocalDateTime.now());
            _logEntries.add(entry);
        }
    }

    /**
     * Use reflection to create a valid status response from an internal class in scheduledtask-core.
     */
    private ScheduleStatus createValidStatus() {
        try {
            Class<?> clazz = Class.forName(SCHEDULE_STATUS_VALID_RESPONSE_CLASS);
            Constructor<?> constructor = clazz.getDeclaredConstructors()[0];
            constructor.setAccessible(true);
            return (ScheduleStatus) constructor.newInstance();
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
            throw new AssertionError("Unable to create ScheduleStatusValidResponse", e);
        }
    }

    // ===== Helpers ===================================================================================================

    /**
     * A mock log line for a given Schedule run.
     */
    static class MockLogEntry implements LogEntry {
        private final long _logId;
        private final long _runId;
        private final String _message;
        private final String _stackTrace;
        private final LocalDateTime _logTime;

        MockLogEntry(long logId, long runId, String message, String stackTrace, LocalDateTime logTime) {
            _logId = logId;
            _runId = runId;
            _message = message;
            _stackTrace = stackTrace;
            _logTime = logTime;
        }

        @Override
        public long getLogId() {
            return _logId;
        }

        @Override
        public long getRunId() {
            return _runId;
        }

        @Override
        public String getMessage() {
            return _message;
        }

        @Override
        public Optional<String> getStackTrace() {
            return Optional.ofNullable(_stackTrace);
        }

        @Override
        public LocalDateTime getLogTime() {
            return _logTime;
        }
    }

    /**
     * Converts a throwable to string.
     */
    static String throwableToStackTraceString(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        // Convert the stackTrace to string;
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        throwable.printStackTrace(pw);
        return sw.toString();
    }
}
