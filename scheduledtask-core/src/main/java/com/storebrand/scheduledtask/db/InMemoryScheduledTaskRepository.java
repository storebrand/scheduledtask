package com.storebrand.scheduledtask.db;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.storebrand.scheduledtask.ScheduledTask.RetentionPolicy;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.LogEntry;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.Schedule;
import com.storebrand.scheduledtask.ScheduledTaskRegistry.State;

/**
 * Simple in-memory implementation for {@link InMemoryScheduledTaskRepository}. This is only intended for use in unit
 * test, and similar testing situations, where we don't want or need to spin up an actual database.
 * <p>
 * Important notice! Locking mechanism that normally ensures only one node runs a scheduled task at a time will not work
 * for this implementation, as there is no communication between service instances. All data is stored in local memory,
 * and is lost when the service stops.
 *
 * @author Kristian Hiim
 */
public class InMemoryScheduledTaskRepository implements ScheduledTaskRepository {

    private final ConcurrentHashMap<String, InMemorySchedule> _schedules = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, ScheduledRunDto> _scheduledRuns = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ScheduledRunDto> _lastRun = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, List<LogEntry>> _logs = new ConcurrentHashMap<>();

    private final Object _lockObject = new Object();
    private final Clock _clock;
    private final AtomicLong _runIdGenerator = new AtomicLong();
    private final AtomicLong _logIdGenerator = new AtomicLong();

    public InMemoryScheduledTaskRepository(Clock clock) {
        _clock = clock;
    }

    @Override
    public int createSchedule(String scheduleName, String cronExpression, Instant nextRun) {
        synchronized (_lockObject) {
            if (_schedules.containsKey(scheduleName)) {
                return 0;
            }
            InMemorySchedule schedule = new InMemorySchedule(scheduleName, cronExpression, true, false,
                    nextRun, _clock.instant());
            _schedules.put(scheduleName, schedule);
            return 1;
        }
    }

    @Override
    public int updateNextRun(String scheduleName, String overrideCronExpression, Instant nextRun) {
        if (createSchedule(scheduleName, overrideCronExpression, nextRun) == 1) {
            return 1;
        }
        synchronized (_lockObject) {
            InMemorySchedule schedule = _schedules.get(scheduleName);
            if (schedule == null) {
                return 0;
            }
            _schedules.put(scheduleName, schedule.update(overrideCronExpression, nextRun, _clock.instant()));
            return 1;
        }
    }

    @Override
    public int setActive(String scheduleName, boolean active) {
        InMemorySchedule schedule = _schedules.get(scheduleName);
        if (schedule == null) {
            return 0;
        }
        _schedules.put(scheduleName, schedule.active(active));
        return 1;
    }

    @Override
    public int setRunOnce(String scheduleName, boolean runOnce) {
        InMemorySchedule schedule = _schedules.get(scheduleName);
        if (schedule == null) {
            return 0;
        }
        _schedules.put(scheduleName, schedule.runOnce(runOnce));
        return 1;
    }

    @Override
    public Map<String, Schedule> getSchedules() {
        return _schedules.entrySet().stream().collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public Optional<Schedule> getSchedule(String scheduleName) {
        return Optional.ofNullable(_schedules.get(scheduleName));
    }

    @Override
    public long addScheduleRun(String scheduleName, String hostname, Instant runStart, String statusMsg) {
        long id = _runIdGenerator.incrementAndGet();
        Instant now = _clock.instant();
        final ScheduledRunDto runDto = new ScheduledRunDto(id, scheduleName, hostname, State.STARTED, statusMsg, null,
                runStart, now);
        _scheduledRuns.put(id, runDto);
        _lastRun.compute(scheduleName, (name, previous) -> {
            if (previous == null) {
                return runDto;
            }
            if (previous.getRunStart().isAfter(runDto.getRunStart())) {
                return previous;
            }
            return runDto;
        });
        return id;
    }

    @Override
    public boolean setStatus(long runId, State state, String statusMsg, String statusStackTrace, Instant statusTime) {
        ScheduledRunDto dto = _scheduledRuns.computeIfPresent(runId, (id, runDto) -> {
            runDto.setStatus(state, statusTime, statusMsg, statusStackTrace);
            return runDto;
        });
        return dto != null;
    }

    @Override
    public boolean setStatus(ScheduledRunDto scheduledRunDto) {
        return setStatus(scheduledRunDto.getRunId(), scheduledRunDto.getStatus(), scheduledRunDto.getStatusMsg(),
                scheduledRunDto.getStatusStackTrace(), scheduledRunDto.getStatusInstant());
    }

    @Override
    public Optional<ScheduledRunDto> getScheduleRun(long runId) {
        return Optional.ofNullable(_scheduledRuns.get(runId));
    }

    @Override
    public Optional<ScheduledRunDto> getLastRunForSchedule(String scheduleName) {
        return Optional.ofNullable(_lastRun.get(scheduleName));
    }

    @Override
    public List<ScheduledRunDto> getLastScheduleRuns() {
        return _lastRun.values().stream()
                .sorted(Comparator.comparing(ScheduledRunDto::getRunStart).reversed())
                .collect(toList());
    }

    @Override
    public List<ScheduledRunDto> getScheduleRunsBetween(String scheduleName, LocalDateTime from, LocalDateTime to) {
        List<ScheduledRunDto> runs = new ArrayList<>();
        for (long i = 0; i <= _runIdGenerator.get(); i++) {
            ScheduledRunDto run = _scheduledRuns.get(i);
            if (run == null) {
                continue;
            }
            LocalDateTime runStart = run.getRunStart();
            if (run.getScheduleName().equals(scheduleName)
                    && (runStart.equals(from) || runStart.isAfter(from))
                    && (runStart.equals(to) || runStart.isBefore(to))) {
                runs.add(run);
            }
        }
        return runs.stream()
                .sorted(Comparator.comparing(ScheduledRunDto::getRunStart).reversed())
                .collect(toList());
    }

    @Override
    public void addLogEntry(long runId, LocalDateTime logTime, String message, String stackTrace) {
        _logs.compute(runId, (id, existingLogs) -> {
            List<LogEntry> logs = existingLogs != null ? existingLogs : new ArrayList<>();
            long logId = _logIdGenerator.incrementAndGet();
            logs.add(new InMemoryLogEntry(logId, id, message, stackTrace, logTime));
            return logs;
        });
    }

    @Override
    public List<LogEntry> getLogEntries(long runId) {
        return _logs.compute(runId, (id, existingLogs) -> existingLogs != null ? existingLogs : new ArrayList<>());
    }

    @Override
    public void executeRetentionPolicy(String scheduleName, RetentionPolicy retentionPolicy) {

        // ?: Is delete runs after days defined?
        if (retentionPolicy.getDeleteRunsAfterDays() > 0) {
            // -> Yes, then we delete all records older than max days.

            LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                    .minusDays(retentionPolicy.getDeleteRunsAfterDays());
            deleteScheduledRuns(scheduleName, run -> run.getRunStart().isBefore(deleteOlder)
                    && (run.getStatus() == State.DONE || run.getStatus() == State.FAILED));
        }

        // ?: Is delete successful runs after days defined?
        if (retentionPolicy.getDeleteSuccessfulRunsAfterDays() > 0) {
            // -> Yes, then we delete all records older than max days.

            LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                    .minusDays(retentionPolicy.getDeleteSuccessfulRunsAfterDays());
            deleteScheduledRuns(scheduleName,
                    run -> run.getRunStart().isBefore(deleteOlder) && run.getStatus() == State.DONE);
        }

        // ?: Is delete failed runs after days defined?
        if (retentionPolicy.getDeleteFailedRunsAfterDays() > 0) {
            // -> Yes, then we delete all records older than max days.

            LocalDateTime deleteOlder = LocalDateTime.now(_clock)
                    .minusDays(retentionPolicy.getDeleteFailedRunsAfterDays());
            deleteScheduledRuns(scheduleName,
                    run -> run.getRunStart().isBefore(deleteOlder) && run.getStatus() == State.FAILED);
        }

        // ?: Have we defined max runs to keep?
        if (retentionPolicy.getKeepMaxRuns() > 0) {
            // -> Yes, then should only keep this many
            keepMaxScheduledRuns(scheduleName, run -> run.getStatus() == State.DONE || run.getStatus() == State.FAILED,
                    retentionPolicy.getKeepMaxRuns());
        }

        // ?: Have we defined max successful runs to keep?
        if (retentionPolicy.getKeepMaxSuccessfulRuns() > 0) {
            // -> Yes, then should only keep this many
            keepMaxScheduledRuns(scheduleName, run -> run.getStatus() == State.DONE,
                    retentionPolicy.getKeepMaxSuccessfulRuns());
        }

        // ?: Have we defined max failed runs to keep?
        if (retentionPolicy.getKeepMaxFailedRuns() > 0) {
            // -> Yes, then should only keep this many
            keepMaxScheduledRuns(scheduleName, run -> run.getStatus() == State.FAILED,
                    retentionPolicy.getKeepMaxFailedRuns());
        }
    }

    private void deleteScheduledRuns(String scheduleName, Predicate<ScheduledRunDto> shouldDelete) {
        List<Long> ids = new ArrayList<>();
        for (ScheduledRunDto run : _scheduledRuns.values()) {
            if (run.getScheduleName().equals(scheduleName) && shouldDelete.test(run)) {
                ids.add(run.getRunId());
            }
        }
        for (long id : ids) {
            _scheduledRuns.remove(id);
            _logs.remove(id);
        }
    }

    private void keepMaxScheduledRuns(String scheduleName, Predicate<ScheduledRunDto> filter, int max) {
        List<ScheduledRunDto> runs = _scheduledRuns.values()
                .stream()
                .filter(run -> run.getScheduleName().equals(scheduleName))
                .filter(filter)
                .sorted(Comparator.comparing(ScheduledRunDto::getRunStart).reversed())
                .collect(toList());
        for (int i = max; i < runs.size(); i++) {
            long id = runs.get(i).getRunId();
            _scheduledRuns.remove(id);
            _logs.remove(id);
        }
    }

    private static final class InMemorySchedule implements Schedule {

        private final String _name;
        private final String _cronExpression;
        private volatile boolean _active;
        private volatile boolean _runOnce;
        private volatile Instant _nextRun;
        private volatile Instant _lastUpdated;

        private InMemorySchedule(String name, String cronExpression, boolean active, boolean runOnce,
                Instant nextRun, Instant lastUpdated) {
            _name = name;
            _cronExpression = cronExpression;
            _active = active;
            _runOnce = runOnce;
            _nextRun = nextRun;
            _lastUpdated = lastUpdated;
        }

        @Override
        public String getName() {
            return _name;
        }

        @Override
        public boolean isActive() {
            return _active;
        }

        @Override
        public boolean isRunOnce() {
            return _runOnce;
        }

        @Override
        public Optional<String> getOverriddenCronExpression() {
            return Optional.ofNullable(_cronExpression);
        }

        @Override
        public Instant getNextRun() {
            return _nextRun;
        }

        @Override
        public Instant getLastUpdated() {
            return _lastUpdated;
        }

        InMemorySchedule active(boolean active) {
            return new InMemorySchedule(_name, _cronExpression, active, _runOnce, _nextRun,
                    _lastUpdated);
        }

        InMemorySchedule runOnce(boolean runOnce) {
            return new InMemorySchedule(_name, _cronExpression, _active, runOnce, _nextRun,
                    _lastUpdated);
        }

        InMemorySchedule update(String overriddenCronExpression, Instant nextRun, Instant lastUpdated) {
            return new InMemorySchedule(_name, overriddenCronExpression, _active, _runOnce, nextRun,
                    lastUpdated);
        }
    }

    /**
     * A log line for a given Schedule run.
     */
    static class InMemoryLogEntry implements LogEntry {
        private final long _logId;
        private final long _runId;
        private final String _message;
        private final String _stackTrace;
        private final LocalDateTime _logTime;

        InMemoryLogEntry(long logId, long runId, String message, String stackTrace, LocalDateTime logTime) {
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
}
