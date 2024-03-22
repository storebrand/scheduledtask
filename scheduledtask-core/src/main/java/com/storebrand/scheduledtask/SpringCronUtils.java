/*
 * Copyright 2002-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.storebrand.scheduledtask;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAdjuster;
import java.time.temporal.TemporalAdjusters;
import java.time.temporal.ValueRange;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.meta.TypeQualifierNickname;
import javax.annotation.meta.When;

/**
 * This class contains code copied from the
 * <a href="https://github.com/spring-projects/spring-framework">Spring Framework</a> in order to support using Cron
 * expressions internally in this library. Classes have been made package private, as they are only meant to be used
 * internally. Some utility classes that were required are also stripped down to the actually required functions.
 *
 * @author the original authors, as mentioned in each specific internal class
 */
class SpringCronUtils {

    // ==== CRON EXPRESSION RELATED CLASSES ============================================================================

    /**
     * Efficient bitwise-operator extension of {@link CronField}.
     * Created using the {@code parse*} methods.
     *
     * @author Arjen Poutsma
     * @since 5.3
     */
    static final class BitsCronField extends CronField {

        private static final long MASK = 0xFFFFFFFFFFFFFFFFL;


        @Nullable
        private static BitsCronField zeroNanos = null;


        // we store at most 60 bits, for seconds and minutes, so a 64-bit long suffices
        private long bits;


        private BitsCronField(Type type) {
            super(type);
        }

        /**
         * Return a {@code BitsCronField} enabled for 0 nano seconds.
         */
        public static BitsCronField zeroNanos() {
            if (zeroNanos == null) {
                BitsCronField field = new BitsCronField(Type.NANO);
                field.setBit(0);
                zeroNanos = field;
            }
            return zeroNanos;
        }

        /**
         * Parse the given value into a seconds {@code BitsCronField}, the first entry of a cron expression.
         */
        public static BitsCronField parseSeconds(String value) {
            return parseField(value, Type.SECOND);
        }

        /**
         * Parse the given value into a minutes {@code BitsCronField}, the second entry of a cron expression.
         */
        public static BitsCronField parseMinutes(String value) {
            return BitsCronField.parseField(value, Type.MINUTE);
        }

        /**
         * Parse the given value into a hours {@code BitsCronField}, the third entry of a cron expression.
         */
        public static BitsCronField parseHours(String value) {
            return BitsCronField.parseField(value, Type.HOUR);
        }

        /**
         * Parse the given value into a days of months {@code BitsCronField}, the fourth entry of a cron expression.
         */
        public static BitsCronField parseDaysOfMonth(String value) {
            return parseDate(value, Type.DAY_OF_MONTH);
        }

        /**
         * Parse the given value into a month {@code BitsCronField}, the fifth entry of a cron expression.
         */
        public static BitsCronField parseMonth(String value) {
            return BitsCronField.parseField(value, Type.MONTH);
        }

        /**
         * Parse the given value into a days of week {@code BitsCronField}, the sixth entry of a cron expression.
         */
        public static BitsCronField parseDaysOfWeek(String value) {
            BitsCronField result = parseDate(value, Type.DAY_OF_WEEK);
            if (result.getBit(0)) {
                // cron supports 0 for Sunday; we use 7 like java.time
                result.setBit(7);
                result.clearBit(0);
            }
            return result;
        }


        private static BitsCronField parseDate(String value, Type type) {
            if (value.equals("?")) {
                value = "*";
            }
            return BitsCronField.parseField(value, type);
        }

        private static BitsCronField parseField(String value, Type type) {
            Assert.hasLength(value, "Value must not be empty");
            Assert.notNull(type, "Type must not be null");
            try {
                BitsCronField result = new BitsCronField(type);
                String[] fields = StringUtils.delimitedListToStringArray(value, ",");
                for (String field : fields) {
                    int slashPos = field.indexOf('/');
                    if (slashPos == -1) {
                        ValueRange range = parseRange(field, type);
                        result.setBits(range);
                    }
                    else {
                        String rangeStr = field.substring(0, slashPos);
                        String deltaStr = field.substring(slashPos + 1);
                        ValueRange range = parseRange(rangeStr, type);
                        if (rangeStr.indexOf('-') == -1) {
                            range = ValueRange.of(range.getMinimum(), type.range().getMaximum());
                        }
                        int delta = Integer.parseInt(deltaStr);
                        if (delta <= 0) {
                            throw new IllegalArgumentException("Incrementer delta must be 1 or higher");
                        }
                        result.setBits(range, delta);
                    }
                }
                return result;
            }
            catch (DateTimeException | IllegalArgumentException ex) {
                String msg = ex.getMessage() + " '" + value + "'";
                throw new IllegalArgumentException(msg, ex);
            }
        }

        private static ValueRange parseRange(String value, Type type) {
            if (value.equals("*")) {
                return type.range();
            }
            else {
                int hyphenPos = value.indexOf('-');
                if (hyphenPos == -1) {
                    int result = type.checkValidValue(Integer.parseInt(value));
                    return ValueRange.of(result, result);
                }
                else {
                    int min = Integer.parseInt(value.substring(0, hyphenPos));
                    int max = Integer.parseInt(value.substring(hyphenPos + 1));
                    min = type.checkValidValue(min);
                    max = type.checkValidValue(max);
                    if (type == Type.DAY_OF_WEEK && min == 7) {
                        // If used as a minimum in a range, Sunday means 0 (not 7)
                        min = 0;
                    }
                    return ValueRange.of(min, max);
                }
            }
        }

        @Nullable
        @Override
        public <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
            int current = type().get(temporal);
            int next = nextSetBit(current);
            if (next == -1) {
                temporal = type().rollForward(temporal);
                next = nextSetBit(0);
            }
            if (next == current) {
                return temporal;
            }
            else {
                int count = 0;
                current = type().get(temporal);
                while (current != next && count++ < CronExpression.MAX_ATTEMPTS) {
                    temporal = type().elapseUntil(temporal, next);
                    current = type().get(temporal);
                    next = nextSetBit(current);
                    if (next == -1) {
                        temporal = type().rollForward(temporal);
                        next = nextSetBit(0);
                    }
                }
                if (count >= CronExpression.MAX_ATTEMPTS) {
                    return null;
                }
                return type().reset(temporal);
            }
        }

        boolean getBit(int index) {
            return (this.bits & (1L << index)) != 0;
        }

        private int nextSetBit(int fromIndex) {
            long result = this.bits & (MASK << fromIndex);
            if (result != 0) {
                return Long.numberOfTrailingZeros(result);
            }
            else {
                return -1;
            }

        }

        private void setBits(ValueRange range) {
            if (range.getMinimum() == range.getMaximum()) {
                setBit((int) range.getMinimum());
            }
            else {
                long minMask = MASK << range.getMinimum();
                long maxMask = MASK >>> - (range.getMaximum() + 1);
                this.bits |= (minMask & maxMask);
            }
        }

        private void setBits(ValueRange range, int delta) {
            if (delta == 1) {
                setBits(range);
            }
            else {
                for (int i = (int) range.getMinimum(); i <= range.getMaximum(); i += delta) {
                    setBit(i);
                }
            }
        }

        private void setBit(int index) {
            this.bits |= (1L << index);
        }

        private void clearBit(int index) {
            this.bits &=  ~(1L << index);
        }

        @Override
        public int hashCode() {
            return Long.hashCode(this.bits);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof BitsCronField)) {
                return false;
            }
            BitsCronField other = (BitsCronField) o;
            return type() == other.type() && this.bits == other.bits;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(type().toString());
            builder.append(" {");
            int i = nextSetBit(0);
            if (i != -1) {
                builder.append(i);
                i = nextSetBit(i+1);
                while (i != -1) {
                    builder.append(", ");
                    builder.append(i);
                    i = nextSetBit(i+1);
                }
            }
            builder.append('}');
            return builder.toString();
        }

    }

    /**
     * Extension of {@link CronField} that wraps an array of cron fields.
     *
     * @author Arjen Poutsma
     * @since 5.3.3
     */
    static final class CompositeCronField extends CronField {

        private final CronField[] fields;

        private final String value;


        private CompositeCronField(Type type, CronField[] fields, String value) {
            super(type);
            this.fields = fields;
            this.value = value;
        }

        /**
         * Composes the given fields into a {@link CronField}.
         */
        public static CronField compose(CronField[] fields, Type type, String value) {
            Assert.notEmpty(fields, "Fields must not be empty");
            Assert.hasLength(value, "Value must not be empty");

            if (fields.length == 1) {
                return fields[0];
            }
            else {
                return new CompositeCronField(type, fields, value);
            }
        }


        @Nullable
        @Override
        public <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
            T result = null;
            for (CronField field : this.fields) {
                T candidate = field.nextOrSame(temporal);
                if (result == null ||
                        candidate != null && candidate.compareTo(result) < 0) {
                    result = candidate;
                }
            }
            return result;
        }


        @Override
        public int hashCode() {
            return this.value.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof CompositeCronField)) {
                return false;
            }
            CompositeCronField other = (CompositeCronField) o;
            return type() == other.type() &&
                    this.value.equals(other.value);
        }

        @Override
        public String toString() {
            return type() + " '" + this.value + "'";

        }
    }

    /**
     * Representation of a
     * <a href="https://www.manpagez.com/man/5/crontab/">crontab expression</a>
     * that can calculate the next time it matches.
     *
     * <p>{@code CronExpression} instances are created through
     * {@link #parse(String)}; the next match is determined with
     * {@link #next(Temporal)}.
     *
     * @author Arjen Poutsma
     * @since 5.3
     * @see CronTrigger
     */
    static final class CronExpression {

        static final int MAX_ATTEMPTS = 366;

        private static final String[] MACROS = new String[] {
                "@yearly", "0 0 0 1 1 *",
                "@annually", "0 0 0 1 1 *",
                "@monthly", "0 0 0 1 * *",
                "@weekly", "0 0 0 * * 0",
                "@daily", "0 0 0 * * *",
                "@midnight", "0 0 0 * * *",
                "@hourly", "0 0 * * * *"
        };


        private final CronField[] fields;

        private final String expression;


        private CronExpression(
                CronField seconds,
                CronField minutes,
                CronField hours,
                CronField daysOfMonth,
                CronField months,
                CronField daysOfWeek,
                String expression) {

            // reverse order, to make big changes first
            // to make sure we end up at 0 nanos, we add an extra field
            this.fields = new CronField[]{daysOfWeek, months, daysOfMonth, hours, minutes, seconds, CronField.zeroNanos()};
            this.expression = expression;
        }


        /**
         * Parse the given
         * <a href="https://www.manpagez.com/man/5/crontab/">crontab expression</a>
         * string into a {@code CronExpression}.
         * The string has six single space-separated time and date fields:
         * <pre>
         * &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; second (0-59)
         * &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; minute (0 - 59)
         * &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; hour (0 - 23)
         * &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; day of the month (1 - 31)
         * &#9474; &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; month (1 - 12) (or JAN-DEC)
         * &#9474; &#9474; &#9474; &#9474; &#9474; &#9484;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472;&#9472; day of the week (0 - 7)
         * &#9474; &#9474; &#9474; &#9474; &#9474; &#9474;          (0 or 7 is Sunday, or MON-SUN)
         * &#9474; &#9474; &#9474; &#9474; &#9474; &#9474;
         * &#42; &#42; &#42; &#42; &#42; &#42;
         * </pre>
         *
         * <p>The following rules apply:
         * <ul>
         * <li>
         * A field may be an asterisk ({@code *}), which always stands for
         * "first-last". For the "day of the month" or "day of the week" fields, a
         * question mark ({@code ?}) may be used instead of an asterisk.
         * </li>
         * <li>
         * Ranges of numbers are expressed by two numbers separated with a hyphen
         * ({@code -}). The specified range is inclusive.
         * </li>
         * <li>Following a range (or {@code *}) with {@code /n} specifies
         * the interval of the number's value through the range.
         * </li>
         * <li>
         * English names can also be used for the "month" and "day of week" fields.
         * Use the first three letters of the particular day or month (case does not
         * matter).
         * </li>
         * <li>
         * The "day of month" and "day of week" fields can contain a
         * {@code L}-character, which stands for "last", and has a different meaning
         * in each field:
         * <ul>
         * <li>
         * In the "day of month" field, {@code L} stands for "the last day of the
         * month". If followed by an negative offset (i.e. {@code L-n}), it means
         * "{@code n}th-to-last day of the month". If followed by {@code W} (i.e.
         * {@code LW}), it means "the last weekday of the month".
         * </li>
         * <li>
         * In the "day of week" field, {@code L} stands for "the last day of the
         * week".
         * If prefixed by a number or three-letter name (i.e. {@code dL} or
         * {@code DDDL}), it means "the last day of week {@code d} (or {@code DDD})
         * in the month".
         * </li>
         * </ul>
         * </li>
         * <li>
         * The "day of month" field can be {@code nW}, which stands for "the nearest
         * weekday to day of the month {@code n}".
         * If {@code n} falls on Saturday, this yields the Friday before it.
         * If {@code n} falls on Sunday, this yields the Monday after,
         * which also happens if {@code n} is {@code 1} and falls on a Saturday
         * (i.e. {@code 1W} stands for "the first weekday of the month").
         * </li>
         * <li>
         * The "day of week" field can be {@code d#n} (or {@code DDD#n}), which
         * stands for "the {@code n}-th day of week {@code d} (or {@code DDD}) in
         * the month".
         * </li>
         * </ul>
         *
         * <p>Example expressions:
         * <ul>
         * <li>{@code "0 0 * * * *"} = the top of every hour of every day.</li>
         * <li><code>"*&#47;10 * * * * *"</code> = every ten seconds.</li>
         * <li>{@code "0 0 8-10 * * *"} = 8, 9 and 10 o'clock of every day.</li>
         * <li>{@code "0 0 6,19 * * *"} = 6:00 AM and 7:00 PM every day.</li>
         * <li>{@code "0 0/30 8-10 * * *"} = 8:00, 8:30, 9:00, 9:30, 10:00 and 10:30 every day.</li>
         * <li>{@code "0 0 9-17 * * MON-FRI"} = on the hour nine-to-five weekdays</li>
         * <li>{@code "0 0 0 25 12 ?"} = every Christmas Day at midnight</li>
         * <li>{@code "0 0 0 L * *"} = last day of the month at midnight</li>
         * <li>{@code "0 0 0 L-3 * *"} = third-to-last day of the month at midnight</li>
         * <li>{@code "0 0 0 1W * *"} = first weekday of the month at midnight</li>
         * <li>{@code "0 0 0 LW * *"} = last weekday of the month at midnight</li>
         * <li>{@code "0 0 0 * * 5L"} = last Friday of the month at midnight</li>
         * <li>{@code "0 0 0 * * THUL"} = last Thursday of the month at midnight</li>
         * <li>{@code "0 0 0 ? * 5#2"} = the second Friday in the month at midnight</li>
         * <li>{@code "0 0 0 ? * MON#1"} = the first Monday in the month at midnight</li>
         * </ul>
         *
         * <p>The following macros are also supported:
         * <ul>
         * <li>{@code "@yearly"} (or {@code "@annually"}) to run un once a year, i.e. {@code "0 0 0 1 1 *"},</li>
         * <li>{@code "@monthly"} to run once a month, i.e. {@code "0 0 0 1 * *"},</li>
         * <li>{@code "@weekly"} to run once a week, i.e. {@code "0 0 0 * * 0"},</li>
         * <li>{@code "@daily"} (or {@code "@midnight"}) to run once a day, i.e. {@code "0 0 0 * * *"},</li>
         * <li>{@code "@hourly"} to run once an hour, i.e. {@code "0 0 * * * *"}.</li>
         * </ul>
         * @param expression the expression string to parse
         * @return the parsed {@code CronExpression} object
         * @throws IllegalArgumentException in the expression does not conform to
         * the cron format
         */
        public static CronExpression parse(String expression) {
            Assert.hasLength(expression, "Expression string must not be empty");

            expression = resolveMacros(expression);

            String[] fields = StringUtils.tokenizeToStringArray(expression, " ");
            if (fields.length != 6) {
                throw new IllegalArgumentException(String.format(
                        "Cron expression must consist of 6 fields (found %d in \"%s\")", fields.length, expression));
            }

            // We only accept 0 as input for the seconds' field.
            String conSeconds = fields[0];
            if (!"0".equals(conSeconds)) {
                throw new IllegalArgumentException(String.format("Cron expression must have seconds field set to 0 "
                        + "(found \"%s\" seconds in \"%s\")", conSeconds, expression));
            }

            try {
                CronField seconds = CronField.parseSeconds(fields[0]);
                CronField minutes = CronField.parseMinutes(fields[1]);
                CronField hours = CronField.parseHours(fields[2]);
                CronField daysOfMonth = CronField.parseDaysOfMonth(fields[3]);
                CronField months = CronField.parseMonth(fields[4]);
                CronField daysOfWeek = CronField.parseDaysOfWeek(fields[5]);

                return new CronExpression(seconds, minutes, hours, daysOfMonth, months, daysOfWeek, expression);
            }
            catch (IllegalArgumentException ex) {
                String msg = ex.getMessage() + " in cron expression \"" + expression + "\"";
                throw new IllegalArgumentException(msg, ex);
            }
        }

        /**
         * Determine whether the given string represents a valid cron expression.
         * @param expression the expression to evaluate
         * @return {@code true} if the given expression is a valid cron expression
         * @since 5.3.8
         */
        public static boolean isValidExpression(@Nullable String expression) {
            if (expression == null) {
                return false;
            }
            try {
                parse(expression);
                return true;
            }
            catch (IllegalArgumentException ex) {
                return false;
            }
        }


        private static String resolveMacros(String expression) {
            expression = expression.trim();
            for (int i = 0; i < MACROS.length; i = i + 2) {
                if (MACROS[i].equalsIgnoreCase(expression)) {
                    return MACROS[i + 1];
                }
            }
            return expression;
        }


        /**
         * Calculate the next {@link Temporal} that matches this expression.
         * @param temporal the seed value
         * @param <T> the type of temporal
         * @return the next temporal that matches this expression, or {@code null}
         * if no such temporal can be found
         */
        @Nullable
        public <T extends Temporal & Comparable<? super T>> T next(T temporal) {
            return nextOrSame(ChronoUnit.NANOS.addTo(temporal, 1));
        }


        @Nullable
        private <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
            for (int i = 0; i < MAX_ATTEMPTS; i++) {
                T result = nextOrSameInternal(temporal);
                if (result == null || result.equals(temporal)) {
                    return result;
                }
                temporal = result;
            }
            return null;
        }

        @Nullable
        private <T extends Temporal & Comparable<? super T>> T nextOrSameInternal(T temporal) {
            for (CronField field : this.fields) {
                temporal = field.nextOrSame(temporal);
                if (temporal == null) {
                    return null;
                }
            }
            return temporal;
        }


        @Override
        public int hashCode() {
            return Arrays.hashCode(this.fields);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o instanceof CronExpression) {
                CronExpression other = (CronExpression) o;
                return Arrays.equals(this.fields, other.fields);
            }
            else {
                return false;
            }
        }

        /**
         * Return the expression string used to create this {@code CronExpression}.
         * @return the expression string
         */
        @Override
        public String toString() {
            return this.expression;
        }

    }

    /**
     * Single field in a cron pattern. Created using the {@code parse*} methods,
     * main and only entry point is {@link #nextOrSame(Temporal)}.
     *
     * @author Arjen Poutsma
     * @since 5.3
     */
    abstract static class CronField {

        private static final String[] MONTHS = new String[]{"JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP",
                "OCT", "NOV", "DEC"};

        private static final String[] DAYS = new String[]{"MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"};

        private final Type type;


        protected CronField(Type type) {
            this.type = type;
        }

        /**
         * Return a {@code CronField} enabled for 0 nano seconds.
         */
        public static CronField zeroNanos() {
            return BitsCronField.zeroNanos();
        }

        /**
         * Parse the given value into a seconds {@code CronField}, the first entry of a cron expression.
         */
        public static CronField parseSeconds(String value) {
            return BitsCronField.parseSeconds(value);
        }

        /**
         * Parse the given value into a minutes {@code CronField}, the second entry of a cron expression.
         */
        public static CronField parseMinutes(String value) {
            return BitsCronField.parseMinutes(value);
        }

        /**
         * Parse the given value into a hours {@code CronField}, the third entry of a cron expression.
         */
        public static CronField parseHours(String value) {
            return BitsCronField.parseHours(value);
        }

        /**
         * Parse the given value into a days of months {@code CronField}, the fourth entry of a cron expression.
         */
        public static CronField parseDaysOfMonth(String value) {
            if (!QuartzCronField.isQuartzDaysOfMonthField(value)) {
                return BitsCronField.parseDaysOfMonth(value);
            }
            else {
                return parseList(value, Type.DAY_OF_MONTH, (field, type) -> {
                    if (QuartzCronField.isQuartzDaysOfMonthField(field)) {
                        return QuartzCronField.parseDaysOfMonth(field);
                    }
                    else {
                        return BitsCronField.parseDaysOfMonth(field);
                    }
                });
            }
        }

        /**
         * Parse the given value into a month {@code CronField}, the fifth entry of a cron expression.
         */
        public static CronField parseMonth(String value) {
            value = replaceOrdinals(value, MONTHS);
            return BitsCronField.parseMonth(value);
        }

        /**
         * Parse the given value into a days of week {@code CronField}, the sixth entry of a cron expression.
         */
        public static CronField parseDaysOfWeek(String value) {
            value = replaceOrdinals(value, DAYS);
            if (!QuartzCronField.isQuartzDaysOfWeekField(value)) {
                return BitsCronField.parseDaysOfWeek(value);
            }
            else {
                return parseList(value, Type.DAY_OF_WEEK, (field, type) -> {
                    if (QuartzCronField.isQuartzDaysOfWeekField(field)) {
                        return QuartzCronField.parseDaysOfWeek(field);
                    }
                    else {
                        return BitsCronField.parseDaysOfWeek(field);
                    }
                });
            }
        }


        private static CronField parseList(String value, Type type, BiFunction<String, Type, CronField> parseFieldFunction) {
            Assert.hasLength(value, "Value must not be empty");
            String[] fields = StringUtils.delimitedListToStringArray(value, ",");
            CronField[] cronFields = new CronField[fields.length];
            for (int i = 0; i < fields.length; i++) {
                cronFields[i] = parseFieldFunction.apply(fields[i], type);
            }
            return CompositeCronField.compose(cronFields, type, value);
        }

        private static String replaceOrdinals(String value, String[] list) {
            value = value.toUpperCase();
            for (int i = 0; i < list.length; i++) {
                String replacement = Integer.toString(i + 1);
                value = StringUtils.replace(value, list[i], replacement);
            }
            return value;
        }


        /**
         * Get the next or same {@link Temporal} in the sequence matching this
         * cron field.
         * @param temporal the seed value
         * @return the next or same temporal matching the pattern
         */
        @Nullable
        public abstract <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal);


        protected Type type() {
            return this.type;
        }

        @SuppressWarnings("unchecked")
        protected static <T extends Temporal & Comparable<? super T>> T cast(Temporal temporal) {
            return (T) temporal;
        }


        /**
         * Represents the type of cron field, i.e. seconds, minutes, hours,
         * day-of-month, month, day-of-week.
         */
        protected enum Type {
            NANO(ChronoField.NANO_OF_SECOND),
            SECOND(ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND),
            MINUTE(ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND),
            HOUR(ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND),
            DAY_OF_MONTH(ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND),
            MONTH(ChronoField.MONTH_OF_YEAR, ChronoField.DAY_OF_MONTH, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND),
            DAY_OF_WEEK(ChronoField.DAY_OF_WEEK, ChronoField.HOUR_OF_DAY, ChronoField.MINUTE_OF_HOUR, ChronoField.SECOND_OF_MINUTE, ChronoField.NANO_OF_SECOND);


            private final ChronoField field;

            private final ChronoField[] lowerOrders;


            Type(ChronoField field, ChronoField... lowerOrders) {
                this.field = field;
                this.lowerOrders = lowerOrders;
            }


            /**
             * Return the value of this type for the given temporal.
             * @return the value of this type
             */
            public int get(Temporal date) {
                return date.get(this.field);
            }

            /**
             * Return the general range of this type. For instance, this methods
             * will return 0-31 for {@link #MONTH}.
             * @return the range of this field
             */
            public ValueRange range() {
                return this.field.range();
            }

            /**
             * Check whether the given value is valid, i.e. whether it falls in
             * {@linkplain #range() range}.
             * @param value the value to check
             * @return the value that was passed in
             * @throws IllegalArgumentException if the given value is invalid
             */
            public int checkValidValue(int value) {
                if (this == DAY_OF_WEEK && value == 0) {
                    return value;
                }
                else {
                    try {
                        return this.field.checkValidIntValue(value);
                    }
                    catch (DateTimeException ex) {
                        throw new IllegalArgumentException(ex.getMessage(), ex);
                    }
                }
            }

            /**
             * Elapse the given temporal for the difference between the current
             * value of this field and the goal value. Typically, the returned
             * temporal will have the given goal as the current value for this type,
             * but this is not the case for {@link #DAY_OF_MONTH}.
             * @param temporal the temporal to elapse
             * @param goal the goal value
             * @param <T> the type of temporal
             * @return the elapsed temporal, typically with {@code goal} as value
             * for this type.
             */
            public <T extends Temporal & Comparable<? super T>> T elapseUntil(T temporal, int goal) {
                int current = get(temporal);
                ValueRange range = temporal.range(this.field);
                if (current < goal) {
                    if (range.isValidIntValue(goal)) {
                        return cast(temporal.with(this.field, goal));
                    }
                    else {
                        // goal is invalid, eg. 29th Feb, so roll forward
                        long amount = range.getMaximum() - current + 1;
                        return this.field.getBaseUnit().addTo(temporal, amount);
                    }
                }
                else {
                    long amount = goal + range.getMaximum() - current + 1 - range.getMinimum();
                    return this.field.getBaseUnit().addTo(temporal, amount);
                }
            }

            /**
             * Roll forward the give temporal until it reaches the next higher
             * order field. Calling this method is equivalent to calling
             * {@link #elapseUntil(Temporal, int)} with goal set to the
             * minimum value of this field's range.
             * @param temporal the temporal to roll forward
             * @param <T> the type of temporal
             * @return the rolled forward temporal
             */
            public <T extends Temporal & Comparable<? super T>> T rollForward(T temporal) {
                int current = get(temporal);
                ValueRange range = temporal.range(this.field);
                long amount = range.getMaximum() - current + 1;
                return this.field.getBaseUnit().addTo(temporal, amount);
            }

            /**
             * Reset this and all lower order fields of the given temporal to their
             * minimum value. For instance for {@link #MINUTE}, this method
             * resets nanos, seconds, <strong>and</strong> minutes to 0.
             * @param temporal the temporal to reset
             * @param <T> the type of temporal
             * @return the reset temporal
             */
            public <T extends Temporal> T reset(T temporal) {
                for (ChronoField lowerOrder : this.lowerOrders) {
                    if (temporal.isSupported(lowerOrder)) {
                        temporal = lowerOrder.adjustInto(temporal, temporal.range(lowerOrder).getMinimum());
                    }
                }
                return temporal;
            }

            @Override
            public String toString() {
                return this.field.toString();
            }
        }

    }

    /**
     * Extension of {@link CronField} for
     * <a href="https://www.quartz-scheduler.org>Quartz</a> -specific fields.
     * Created using the {@code parse*} methods, uses a {@link TemporalAdjuster}
     * internally.
     *
     * @author Arjen Poutsma
     * @since 5.3
     */
    static final class QuartzCronField extends CronField {

        private final Type rollForwardType;

        private final TemporalAdjuster adjuster;

        private final String value;


        private QuartzCronField(Type type, TemporalAdjuster adjuster, String value) {
            this(type, type, adjuster, value);
        }

        /**
         * Constructor for fields that need to roll forward over a different type
         * than the type this field represents. See {@link #parseDaysOfWeek(String)}.
         */
        private QuartzCronField(Type type, Type rollForwardType, TemporalAdjuster adjuster, String value) {
            super(type);
            this.adjuster = adjuster;
            this.value = value;
            this.rollForwardType = rollForwardType;
        }

        /**
         * Returns whether the given value is a Quartz day-of-month field.
         */
        public static boolean isQuartzDaysOfMonthField(String value) {
            return value.contains("L") || value.contains("W");
        }

        /**
         * Parse the given value into a days of months {@code QuartzCronField}, the fourth entry of a cron expression.
         * Expects a "L" or "W" in the given value.
         */
        public static QuartzCronField parseDaysOfMonth(String value) {
            int idx = value.lastIndexOf('L');
            if (idx != -1) {
                TemporalAdjuster adjuster;
                if (idx != 0) {
                    throw new IllegalArgumentException("Unrecognized characters before 'L' in '" + value + "'");
                }
                else if (value.length() == 2 && value.charAt(1) == 'W') { // "LW"
                    adjuster = lastWeekdayOfMonth();
                }
                else {
                    if (value.length() == 1) { // "L"
                        adjuster = lastDayOfMonth();
                    }
                    else { // "L-[0-9]+"
                        int offset = Integer.parseInt(value.substring(idx + 1));
                        if (offset >= 0) {
                            throw new IllegalArgumentException("Offset '" + offset + " should be < 0 '" + value + "'");
                        }
                        adjuster = lastDayWithOffset(offset);
                    }
                }
                return new QuartzCronField(Type.DAY_OF_MONTH, adjuster, value);
            }
            idx = value.lastIndexOf('W');
            if (idx != -1) {
                if (idx == 0) {
                    throw new IllegalArgumentException("No day-of-month before 'W' in '" + value + "'");
                }
                else if (idx != value.length() - 1) {
                    throw new IllegalArgumentException("Unrecognized characters after 'W' in '" + value + "'");
                }
                else { // "[0-9]+W"
                    int dayOfMonth = Integer.parseInt(value.substring(0, idx));
                    dayOfMonth = Type.DAY_OF_MONTH.checkValidValue(dayOfMonth);
                    TemporalAdjuster adjuster = weekdayNearestTo(dayOfMonth);
                    return new QuartzCronField(Type.DAY_OF_MONTH, adjuster, value);
                }
            }
            throw new IllegalArgumentException("No 'L' or 'W' found in '" + value + "'");
        }

        /**
         * Returns whether the given value is a Quartz day-of-week field.
         */
        public static boolean isQuartzDaysOfWeekField(String value) {
            return value.contains("L") || value.contains("#");
        }

        /**
         * Parse the given value into a days of week {@code QuartzCronField}, the sixth entry of a cron expression.
         * Expects a "L" or "#" in the given value.
         */
        public static QuartzCronField parseDaysOfWeek(String value) {
            int idx = value.lastIndexOf('L');
            if (idx != -1) {
                if (idx != value.length() - 1) {
                    throw new IllegalArgumentException("Unrecognized characters after 'L' in '" + value + "'");
                }
                else {
                    TemporalAdjuster adjuster;
                    if (idx == 0) {
                        throw new IllegalArgumentException("No day-of-week before 'L' in '" + value + "'");
                    }
                    else { // "[0-7]L"
                        DayOfWeek dayOfWeek = parseDayOfWeek(value.substring(0, idx));
                        adjuster = lastInMonth(dayOfWeek);
                    }
                    return new QuartzCronField(Type.DAY_OF_WEEK, Type.DAY_OF_MONTH, adjuster, value);
                }
            }
            idx = value.lastIndexOf('#');
            if (idx != -1) {
                if (idx == 0) {
                    throw new IllegalArgumentException("No day-of-week before '#' in '" + value + "'");
                }
                else if (idx == value.length() - 1) {
                    throw new IllegalArgumentException("No ordinal after '#' in '" + value + "'");
                }
                // "[0-7]#[0-9]+"
                DayOfWeek dayOfWeek = parseDayOfWeek(value.substring(0, idx));
                int ordinal = Integer.parseInt(value.substring(idx + 1));
                if (ordinal <= 0) {
                    throw new IllegalArgumentException("Ordinal '" + ordinal + "' in '" + value +
                            "' must be positive number ");
                }

                TemporalAdjuster adjuster = dayOfWeekInMonth(ordinal, dayOfWeek);
                return new QuartzCronField(Type.DAY_OF_WEEK, Type.DAY_OF_MONTH, adjuster, value);
            }
            throw new IllegalArgumentException("No 'L' or '#' found in '" + value + "'");
        }

        private static DayOfWeek parseDayOfWeek(String value) {
            int dayOfWeek = Integer.parseInt(value);
            if (dayOfWeek == 0) {
                dayOfWeek = 7; // cron is 0 based; java.time 1 based
            }
            try {
                return DayOfWeek.of(dayOfWeek);
            }
            catch (DateTimeException ex) {
                String msg = ex.getMessage() + " '" + value + "'";
                throw new IllegalArgumentException(msg, ex);
            }
        }

        /**
         * Returns an adjuster that resets to midnight.
         */
        private static TemporalAdjuster atMidnight() {
            return temporal -> {
                if (temporal.isSupported(ChronoField.NANO_OF_DAY)) {
                    return temporal.with(ChronoField.NANO_OF_DAY, 0);
                }
                else {
                    return temporal;
                }
            };
        }

        /**
         * Returns an adjuster that returns a new temporal set to the last
         * day of the current month at midnight.
         */
        private static TemporalAdjuster lastDayOfMonth() {
            TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
            return temporal -> {
                Temporal result = adjuster.adjustInto(temporal);
                return rollbackToMidnight(temporal, result);
            };
        }

        /**
         * Returns an adjuster that returns the last weekday of the month.
         */
        private static TemporalAdjuster lastWeekdayOfMonth() {
            TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
            return temporal -> {
                Temporal lastDom = adjuster.adjustInto(temporal);
                Temporal result;
                int dow = lastDom.get(ChronoField.DAY_OF_WEEK);
                if (dow == 6) { // Saturday
                    result = lastDom.minus(1, ChronoUnit.DAYS);
                }
                else if (dow == 7) { // Sunday
                    result = lastDom.minus(2, ChronoUnit.DAYS);
                }
                else {
                    result = lastDom;
                }
                return rollbackToMidnight(temporal, result);
            };
        }

        /**
         * Return a temporal adjuster that finds the nth-to-last day of the month.
         * @param offset the negative offset, i.e. -3 means third-to-last
         * @return a nth-to-last day-of-month adjuster
         */
        private static TemporalAdjuster lastDayWithOffset(int offset) {
            Assert.isTrue(offset < 0, "Offset should be < 0");
            TemporalAdjuster adjuster = TemporalAdjusters.lastDayOfMonth();
            return temporal -> {
                Temporal result = adjuster.adjustInto(temporal).plus(offset, ChronoUnit.DAYS);
                return rollbackToMidnight(temporal, result);
            };
        }

        /**
         * Return a temporal adjuster that finds the weekday nearest to the given
         * day-of-month. If {@code dayOfMonth} falls on a Saturday, the date is
         * moved back to Friday; if it falls on a Sunday (or if {@code dayOfMonth}
         * is 1 and it falls on a Saturday), it is moved forward to Monday.
         * @param dayOfMonth the goal day-of-month
         * @return the weekday-nearest-to adjuster
         */
        private static TemporalAdjuster weekdayNearestTo(int dayOfMonth) {
            return temporal -> {
                int current = Type.DAY_OF_MONTH.get(temporal);
                int dayOfWeek = temporal.get(ChronoField.DAY_OF_WEEK);

                if ((current == dayOfMonth && dayOfWeek < 6) || // dayOfMonth is a weekday
                        (dayOfWeek == 5 && current == dayOfMonth - 1) || // dayOfMonth is a Saturday, so Friday before
                        (dayOfWeek == 1 && current == dayOfMonth + 1) || // dayOfMonth is a Sunday, so Monday after
                        (dayOfWeek == 1 && dayOfMonth == 1 && current == 3)) { // dayOfMonth is the 1st, so Monday 3rd
                    return temporal;
                }
                int count = 0;
                while (count++ < CronExpression.MAX_ATTEMPTS) {
                    temporal = Type.DAY_OF_MONTH.elapseUntil(cast(temporal), dayOfMonth);
                    temporal = atMidnight().adjustInto(temporal);
                    current = Type.DAY_OF_MONTH.get(temporal);
                    if (current == dayOfMonth) {
                        dayOfWeek = temporal.get(ChronoField.DAY_OF_WEEK);

                        if (dayOfWeek == 6) { // Saturday
                            if (dayOfMonth != 1) {
                                return temporal.minus(1, ChronoUnit.DAYS);
                            }
                            else {
                                // exception for "1W" fields: execute on nearest Monday
                                return temporal.plus(2, ChronoUnit.DAYS);
                            }
                        }
                        else if (dayOfWeek == 7) { // Sunday
                            return temporal.plus(1, ChronoUnit.DAYS);
                        }
                        else {
                            return temporal;
                        }
                    }
                }
                return null;
            };
        }

        /**
         * Return a temporal adjuster that finds the last of the given doy-of-week
         * in a month.
         */
        private static TemporalAdjuster lastInMonth(DayOfWeek dayOfWeek) {
            TemporalAdjuster adjuster = TemporalAdjusters.lastInMonth(dayOfWeek);
            return temporal -> {
                Temporal result = adjuster.adjustInto(temporal);
                return rollbackToMidnight(temporal, result);
            };
        }

        /**
         * Returns a temporal adjuster that finds {@code ordinal}-th occurrence of
         * the given day-of-week in a month.
         */
        private static TemporalAdjuster dayOfWeekInMonth(int ordinal, DayOfWeek dayOfWeek) {
            TemporalAdjuster adjuster = TemporalAdjusters.dayOfWeekInMonth(ordinal, dayOfWeek);
            return temporal -> {
                Temporal result = adjuster.adjustInto(temporal);
                return rollbackToMidnight(temporal, result);
            };
        }

        /**
         * Rolls back the given {@code result} to midnight. When
         * {@code current} has the same day of month as {@code result}, the former
         * is returned, to make sure that we don't end up before where we started.
         */
        private static Temporal rollbackToMidnight(Temporal current, Temporal result) {
            if (result.get(ChronoField.DAY_OF_MONTH) == current.get(ChronoField.DAY_OF_MONTH)) {
                return current;
            }
            else {
                return atMidnight().adjustInto(result);
            }
        }

        @Override
        public <T extends Temporal & Comparable<? super T>> T nextOrSame(T temporal) {
            T result = adjust(temporal);
            if (result != null) {
                if (result.compareTo(temporal) < 0) {
                    // We ended up before the start, roll forward and try again
                    temporal = this.rollForwardType.rollForward(temporal);
                    result = adjust(temporal);
                    if (result != null) {
                        result = type().reset(result);
                    }
                }
            }
            return result;
        }


        @Nullable
        @SuppressWarnings("unchecked")
        private <T extends Temporal & Comparable<? super T>> T adjust(T temporal) {
            return (T) this.adjuster.adjustInto(temporal);
        }


        @Override
        public int hashCode() {
            return this.value.hashCode();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof QuartzCronField)) {
                return false;
            }
            QuartzCronField other = (QuartzCronField) o;
            return type() == other.type() &&
                    this.value.equals(other.value);
        }

        @Override
        public String toString() {
            return type() + " '" + this.value + "'";

        }

    }

    // ==== UTILITY CLASSES REQUIRED BY CRON EXPRESSION ================================================================

    /**
     * Assertion utility class that assists in validating arguments.
     *
     * <p>Useful for identifying programmer errors early and clearly at runtime.
     *
     * <p>For example, if the contract of a public method states it does not
     * allow {@code null} arguments, {@code Assert} can be used to validate that
     * contract. Doing this clearly indicates a contract violation when it
     * occurs and protects the class's invariants.
     *
     * <p>Typically used to validate method arguments rather than configuration
     * properties, to check for cases that are usually programmer errors rather
     * than configuration errors. In contrast to configuration initialization
     * code, there is usually no point in falling back to defaults in such methods.
     *
     * <p>This class is similar to JUnit's assertion library. If an argument value is
     * deemed invalid, an {@link IllegalArgumentException} is thrown (typically).
     * For example:
     *
     * <pre class="code">
     * Assert.notNull(clazz, "The class must not be null");
     * Assert.isTrue(i &gt; 0, "The value must be greater than zero");</pre>
     *
     * <p>Mainly for internal use within the framework; for a more comprehensive suite
     * of assertion utilities consider {@code org.apache.commons.lang3.Validate} from
     * <a href="https://commons.apache.org/proper/commons-lang/">Apache Commons Lang</a>,
     * Google Guava's
     * <a href="https://github.com/google/guava/wiki/PreconditionsExplained">Preconditions</a>,
     * or similar third-party libraries.
     *
     * <p>This file was copied as-is from Spring Framework v5.3.15 package "org.springframework.util",
     * except for this comment, and that it is placed in a new package.
     *
     * @author Keith Donald
     * @author Juergen Hoeller
     * @author Sam Brannen
     * @author Colin Sampaleanu
     * @author Rob Harrop
     * @since 1.1.2
     */
    abstract static class Assert {

        /**
         * Assert a boolean expression, throwing an {@code IllegalStateException}
         * if the expression evaluates to {@code false}.
         * <p>Call {@link #isTrue} if you wish to throw an {@code IllegalArgumentException}
         * on an assertion failure.
         * <pre class="code">Assert.state(id == null, "The id property must not already be initialized");</pre>
         * @param expression a boolean expression
         * @param message the exception message to use if the assertion fails
         * @throws IllegalStateException if {@code expression} is {@code false}
         */
        public static void state(boolean expression, String message) {
            if (!expression) {
                throw new IllegalStateException(message);
            }
        }

        /**
         * Assert a boolean expression, throwing an {@code IllegalStateException}
         * if the expression evaluates to {@code false}.
         * <p>Call {@link #isTrue} if you wish to throw an {@code IllegalArgumentException}
         * on an assertion failure.
         * <pre class="code">
         * Assert.state(entity.getId() == null,
         *     () -&gt; "ID for entity " + entity.getName() + " must not already be initialized");
         * </pre>
         * @param expression a boolean expression
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalStateException if {@code expression} is {@code false}
         * @since 5.0
         */
        public static void state(boolean expression, Supplier<String> messageSupplier) {
            if (!expression) {
                throw new IllegalStateException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert a boolean expression, throwing an {@code IllegalStateException}
         * if the expression evaluates to {@code false}.
         * @deprecated as of 4.3.7, in favor of {@link #state(boolean, String)}
         */
        @Deprecated
        public static void state(boolean expression) {
            state(expression, "[Assertion failed] - this state invariant must be true");
        }

        /**
         * Assert a boolean expression, throwing an {@code IllegalArgumentException}
         * if the expression evaluates to {@code false}.
         * <pre class="code">Assert.isTrue(i &gt; 0, "The value must be greater than zero");</pre>
         * @param expression a boolean expression
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if {@code expression} is {@code false}
         */
        public static void isTrue(boolean expression, String message) {
            if (!expression) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert a boolean expression, throwing an {@code IllegalArgumentException}
         * if the expression evaluates to {@code false}.
         * <pre class="code">
         * Assert.isTrue(i &gt; 0, () -&gt; "The value '" + i + "' must be greater than zero");
         * </pre>
         * @param expression a boolean expression
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if {@code expression} is {@code false}
         * @since 5.0
         */
        public static void isTrue(boolean expression, Supplier<String> messageSupplier) {
            if (!expression) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert a boolean expression, throwing an {@code IllegalArgumentException}
         * if the expression evaluates to {@code false}.
         * @deprecated as of 4.3.7, in favor of {@link #isTrue(boolean, String)}
         */
        @Deprecated
        public static void isTrue(boolean expression) {
            isTrue(expression, "[Assertion failed] - this expression must be true");
        }

        /**
         * Assert that an object is {@code null}.
         * <pre class="code">Assert.isNull(value, "The value must be null");</pre>
         * @param object the object to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the object is not {@code null}
         */
        public static void isNull(@Nullable Object object, String message) {
            if (object != null) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that an object is {@code null}.
         * <pre class="code">
         * Assert.isNull(value, () -&gt; "The value '" + value + "' must be null");
         * </pre>
         * @param object the object to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the object is not {@code null}
         * @since 5.0
         */
        public static void isNull(@Nullable Object object, Supplier<String> messageSupplier) {
            if (object != null) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that an object is {@code null}.
         * @deprecated as of 4.3.7, in favor of {@link #isNull(Object, String)}
         */
        @Deprecated
        public static void isNull(@Nullable Object object) {
            isNull(object, "[Assertion failed] - the object argument must be null");
        }

        /**
         * Assert that an object is not {@code null}.
         * <pre class="code">Assert.notNull(clazz, "The class must not be null");</pre>
         * @param object the object to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the object is {@code null}
         */
        public static void notNull(@Nullable Object object, String message) {
            if (object == null) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that an object is not {@code null}.
         * <pre class="code">
         * Assert.notNull(entity.getId(),
         *     () -&gt; "ID for entity " + entity.getName() + " must not be null");
         * </pre>
         * @param object the object to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the object is {@code null}
         * @since 5.0
         */
        public static void notNull(@Nullable Object object, Supplier<String> messageSupplier) {
            if (object == null) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that an object is not {@code null}.
         * @deprecated as of 4.3.7, in favor of {@link #notNull(Object, String)}
         */
        @Deprecated
        public static void notNull(@Nullable Object object) {
            notNull(object, "[Assertion failed] - this argument is required; it must not be null");
        }

        /**
         * Assert that the given String is not empty; that is,
         * it must not be {@code null} and not the empty String.
         * <pre class="code">Assert.hasLength(name, "Name must not be empty");</pre>
         * @param text the String to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the text is empty
         * @see StringUtils#hasLength
         */
        public static void hasLength(@Nullable String text, String message) {
            if (!StringUtils.hasLength(text)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that the given String is not empty; that is,
         * it must not be {@code null} and not the empty String.
         * <pre class="code">
         * Assert.hasLength(account.getName(),
         *     () -&gt; "Name for account '" + account.getId() + "' must not be empty");
         * </pre>
         * @param text the String to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the text is empty
         * @since 5.0
         * @see StringUtils#hasLength
         */
        public static void hasLength(@Nullable String text, Supplier<String> messageSupplier) {
            if (!StringUtils.hasLength(text)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that the given String is not empty; that is,
         * it must not be {@code null} and not the empty String.
         * @deprecated as of 4.3.7, in favor of {@link #hasLength(String, String)}
         */
        @Deprecated
        public static void hasLength(@Nullable String text) {
            hasLength(text,
                    "[Assertion failed] - this String argument must have length; it must not be null or empty");
        }

        /**
         * Assert that the given String contains valid text content; that is, it must not
         * be {@code null} and must contain at least one non-whitespace character.
         * <pre class="code">Assert.hasText(name, "'name' must not be empty");</pre>
         * @param text the String to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the text does not contain valid text content
         * @see StringUtils#hasText
         */
        public static void hasText(@Nullable String text, String message) {
            if (!StringUtils.hasText(text)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that the given String contains valid text content; that is, it must not
         * be {@code null} and must contain at least one non-whitespace character.
         * <pre class="code">
         * Assert.hasText(account.getName(),
         *     () -&gt; "Name for account '" + account.getId() + "' must not be empty");
         * </pre>
         * @param text the String to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the text does not contain valid text content
         * @since 5.0
         * @see StringUtils#hasText
         */
        public static void hasText(@Nullable String text, Supplier<String> messageSupplier) {
            if (!StringUtils.hasText(text)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that the given String contains valid text content; that is, it must not
         * be {@code null} and must contain at least one non-whitespace character.
         * @deprecated as of 4.3.7, in favor of {@link #hasText(String, String)}
         */
        @Deprecated
        public static void hasText(@Nullable String text) {
            hasText(text,
                    "[Assertion failed] - this String argument must have text; it must not be null, empty, or blank");
        }

        /**
         * Assert that the given text does not contain the given substring.
         * <pre class="code">Assert.doesNotContain(name, "rod", "Name must not contain 'rod'");</pre>
         * @param textToSearch the text to search
         * @param substring the substring to find within the text
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the text contains the substring
         */
        public static void doesNotContain(@Nullable String textToSearch, String substring, String message) {
            if (StringUtils.hasLength(textToSearch) && StringUtils.hasLength(substring) &&
                    textToSearch.contains(substring)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that the given text does not contain the given substring.
         * <pre class="code">
         * Assert.doesNotContain(name, forbidden, () -&gt; "Name must not contain '" + forbidden + "'");
         * </pre>
         * @param textToSearch the text to search
         * @param substring the substring to find within the text
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the text contains the substring
         * @since 5.0
         */
        public static void doesNotContain(@Nullable String textToSearch, String substring, Supplier<String> messageSupplier) {
            if (StringUtils.hasLength(textToSearch) && StringUtils.hasLength(substring) &&
                    textToSearch.contains(substring)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that the given text does not contain the given substring.
         * @deprecated as of 4.3.7, in favor of {@link #doesNotContain(String, String, String)}
         */
        @Deprecated
        public static void doesNotContain(@Nullable String textToSearch, String substring) {
            doesNotContain(textToSearch, substring,
                    () -> "[Assertion failed] - this String argument must not contain the substring [" + substring + "]");
        }

        /**
         * Assert that an array contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * <pre class="code">Assert.notEmpty(array, "The array must contain elements");</pre>
         * @param array the array to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the object array is {@code null} or contains no elements
         */
        public static void notEmpty(@Nullable Object[] array, String message) {
            if (ObjectUtils.isEmpty(array)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that an array contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * <pre class="code">
         * Assert.notEmpty(array, () -&gt; "The " + arrayType + " array must contain elements");
         * </pre>
         * @param array the array to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the object array is {@code null} or contains no elements
         * @since 5.0
         */
        public static void notEmpty(@Nullable Object[] array, Supplier<String> messageSupplier) {
            if (ObjectUtils.isEmpty(array)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that an array contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * @deprecated as of 4.3.7, in favor of {@link #notEmpty(Object[], String)}
         */
        @Deprecated
        public static void notEmpty(@Nullable Object[] array) {
            notEmpty(array, "[Assertion failed] - this array must not be empty: it must contain at least 1 element");
        }

        /**
         * Assert that an array contains no {@code null} elements.
         * <p>Note: Does not complain if the array is empty!
         * <pre class="code">Assert.noNullElements(array, "The array must contain non-null elements");</pre>
         * @param array the array to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the object array contains a {@code null} element
         */
        public static void noNullElements(@Nullable Object[] array, String message) {
            if (array != null) {
                for (Object element : array) {
                    if (element == null) {
                        throw new IllegalArgumentException(message);
                    }
                }
            }
        }

        /**
         * Assert that an array contains no {@code null} elements.
         * <p>Note: Does not complain if the array is empty!
         * <pre class="code">
         * Assert.noNullElements(array, () -&gt; "The " + arrayType + " array must contain non-null elements");
         * </pre>
         * @param array the array to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the object array contains a {@code null} element
         * @since 5.0
         */
        public static void noNullElements(@Nullable Object[] array, Supplier<String> messageSupplier) {
            if (array != null) {
                for (Object element : array) {
                    if (element == null) {
                        throw new IllegalArgumentException(nullSafeGet(messageSupplier));
                    }
                }
            }
        }

        /**
         * Assert that an array contains no {@code null} elements.
         * @deprecated as of 4.3.7, in favor of {@link #noNullElements(Object[], String)}
         */
        @Deprecated
        public static void noNullElements(@Nullable Object[] array) {
            noNullElements(array, "[Assertion failed] - this array must not contain any null elements");
        }

        /**
         * Assert that a collection contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * <pre class="code">Assert.notEmpty(collection, "Collection must contain elements");</pre>
         * @param collection the collection to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the collection is {@code null} or
         * contains no elements
         */
        public static void notEmpty(@Nullable Collection<?> collection, String message) {
            if (CollectionUtils.isEmpty(collection)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that a collection contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * <pre class="code">
         * Assert.notEmpty(collection, () -&gt; "The " + collectionType + " collection must contain elements");
         * </pre>
         * @param collection the collection to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the collection is {@code null} or
         * contains no elements
         * @since 5.0
         */
        public static void notEmpty(@Nullable Collection<?> collection, Supplier<String> messageSupplier) {
            if (CollectionUtils.isEmpty(collection)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that a collection contains elements; that is, it must not be
         * {@code null} and must contain at least one element.
         * @deprecated as of 4.3.7, in favor of {@link #notEmpty(Collection, String)}
         */
        @Deprecated
        public static void notEmpty(@Nullable Collection<?> collection) {
            notEmpty(collection,
                    "[Assertion failed] - this collection must not be empty: it must contain at least 1 element");
        }

        /**
         * Assert that a collection contains no {@code null} elements.
         * <p>Note: Does not complain if the collection is empty!
         * <pre class="code">Assert.noNullElements(collection, "Collection must contain non-null elements");</pre>
         * @param collection the collection to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the collection contains a {@code null} element
         * @since 5.2
         */
        public static void noNullElements(@Nullable Collection<?> collection, String message) {
            if (collection != null) {
                for (Object element : collection) {
                    if (element == null) {
                        throw new IllegalArgumentException(message);
                    }
                }
            }
        }

        /**
         * Assert that a collection contains no {@code null} elements.
         * <p>Note: Does not complain if the collection is empty!
         * <pre class="code">
         * Assert.noNullElements(collection, () -&gt; "Collection " + collectionName + " must contain non-null elements");
         * </pre>
         * @param collection the collection to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the collection contains a {@code null} element
         * @since 5.2
         */
        public static void noNullElements(@Nullable Collection<?> collection, Supplier<String> messageSupplier) {
            if (collection != null) {
                for (Object element : collection) {
                    if (element == null) {
                        throw new IllegalArgumentException(nullSafeGet(messageSupplier));
                    }
                }
            }
        }

        /**
         * Assert that a Map contains entries; that is, it must not be {@code null}
         * and must contain at least one entry.
         * <pre class="code">Assert.notEmpty(map, "Map must contain entries");</pre>
         * @param map the map to check
         * @param message the exception message to use if the assertion fails
         * @throws IllegalArgumentException if the map is {@code null} or contains no entries
         */
        public static void notEmpty(@Nullable Map<?, ?> map, String message) {
            if (CollectionUtils.isEmpty(map)) {
                throw new IllegalArgumentException(message);
            }
        }

        /**
         * Assert that a Map contains entries; that is, it must not be {@code null}
         * and must contain at least one entry.
         * <pre class="code">
         * Assert.notEmpty(map, () -&gt; "The " + mapType + " map must contain entries");
         * </pre>
         * @param map the map to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails
         * @throws IllegalArgumentException if the map is {@code null} or contains no entries
         * @since 5.0
         */
        public static void notEmpty(@Nullable Map<?, ?> map, Supplier<String> messageSupplier) {
            if (CollectionUtils.isEmpty(map)) {
                throw new IllegalArgumentException(nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that a Map contains entries; that is, it must not be {@code null}
         * and must contain at least one entry.
         * @deprecated as of 4.3.7, in favor of {@link #notEmpty(Map, String)}
         */
        @Deprecated
        public static void notEmpty(@Nullable Map<?, ?> map) {
            notEmpty(map, "[Assertion failed] - this map must not be empty; it must contain at least one entry");
        }

        /**
         * Assert that the provided object is an instance of the provided class.
         * <pre class="code">Assert.instanceOf(Foo.class, foo, "Foo expected");</pre>
         * @param type the type to check against
         * @param obj the object to check
         * @param message a message which will be prepended to provide further context.
         * If it is empty or ends in ":" or ";" or "," or ".", a full exception message
         * will be appended. If it ends in a space, the name of the offending object's
         * type will be appended. In any other case, a ":" with a space and the name
         * of the offending object's type will be appended.
         * @throws IllegalArgumentException if the object is not an instance of type
         */
        public static void isInstanceOf(Class<?> type, @Nullable Object obj, String message) {
            notNull(type, "Type to check against must not be null");
            if (!type.isInstance(obj)) {
                instanceCheckFailed(type, obj, message);
            }
        }

        /**
         * Assert that the provided object is an instance of the provided class.
         * <pre class="code">
         * Assert.instanceOf(Foo.class, foo, () -&gt; "Processing " + Foo.class.getSimpleName() + ":");
         * </pre>
         * @param type the type to check against
         * @param obj the object to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails. See {@link #isInstanceOf(Class, Object, String)} for details.
         * @throws IllegalArgumentException if the object is not an instance of type
         * @since 5.0
         */
        public static void isInstanceOf(Class<?> type, @Nullable Object obj, Supplier<String> messageSupplier) {
            notNull(type, "Type to check against must not be null");
            if (!type.isInstance(obj)) {
                instanceCheckFailed(type, obj, nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that the provided object is an instance of the provided class.
         * <pre class="code">Assert.instanceOf(Foo.class, foo);</pre>
         * @param type the type to check against
         * @param obj the object to check
         * @throws IllegalArgumentException if the object is not an instance of type
         */
        public static void isInstanceOf(Class<?> type, @Nullable Object obj) {
            isInstanceOf(type, obj, "");
        }

        /**
         * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
         * <pre class="code">Assert.isAssignable(Number.class, myClass, "Number expected");</pre>
         * @param superType the super type to check against
         * @param subType the sub type to check
         * @param message a message which will be prepended to provide further context.
         * If it is empty or ends in ":" or ";" or "," or ".", a full exception message
         * will be appended. If it ends in a space, the name of the offending sub type
         * will be appended. In any other case, a ":" with a space and the name of the
         * offending sub type will be appended.
         * @throws IllegalArgumentException if the classes are not assignable
         */
        public static void isAssignable(Class<?> superType, @Nullable Class<?> subType, String message) {
            notNull(superType, "Super type to check against must not be null");
            if (subType == null || !superType.isAssignableFrom(subType)) {
                assignableCheckFailed(superType, subType, message);
            }
        }

        /**
         * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
         * <pre class="code">
         * Assert.isAssignable(Number.class, myClass, () -&gt; "Processing " + myAttributeName + ":");
         * </pre>
         * @param superType the super type to check against
         * @param subType the sub type to check
         * @param messageSupplier a supplier for the exception message to use if the
         * assertion fails. See {@link #isAssignable(Class, Class, String)} for details.
         * @throws IllegalArgumentException if the classes are not assignable
         * @since 5.0
         */
        public static void isAssignable(Class<?> superType, @Nullable Class<?> subType, Supplier<String> messageSupplier) {
            notNull(superType, "Super type to check against must not be null");
            if (subType == null || !superType.isAssignableFrom(subType)) {
                assignableCheckFailed(superType, subType, nullSafeGet(messageSupplier));
            }
        }

        /**
         * Assert that {@code superType.isAssignableFrom(subType)} is {@code true}.
         * <pre class="code">Assert.isAssignable(Number.class, myClass);</pre>
         * @param superType the super type to check
         * @param subType the sub type to check
         * @throws IllegalArgumentException if the classes are not assignable
         */
        public static void isAssignable(Class<?> superType, Class<?> subType) {
            isAssignable(superType, subType, "");
        }


        private static void instanceCheckFailed(Class<?> type, @Nullable Object obj, @Nullable String msg) {
            String className = (obj != null ? obj.getClass().getName() : "null");
            String result = "";
            boolean defaultMessage = true;
            if (StringUtils.hasLength(msg)) {
                if (endsWithSeparator(msg)) {
                    result = msg + " ";
                }
                else {
                    result = messageWithTypeName(msg, className);
                    defaultMessage = false;
                }
            }
            if (defaultMessage) {
                result = result + ("Object of class [" + className + "] must be an instance of " + type);
            }
            throw new IllegalArgumentException(result);
        }

        private static void assignableCheckFailed(Class<?> superType, @Nullable Class<?> subType, @Nullable String msg) {
            String result = "";
            boolean defaultMessage = true;
            if (StringUtils.hasLength(msg)) {
                if (endsWithSeparator(msg)) {
                    result = msg + " ";
                }
                else {
                    result = messageWithTypeName(msg, subType);
                    defaultMessage = false;
                }
            }
            if (defaultMessage) {
                result = result + (subType + " is not assignable to " + superType);
            }
            throw new IllegalArgumentException(result);
        }

        private static boolean endsWithSeparator(String msg) {
            return (msg.endsWith(":") || msg.endsWith(";") || msg.endsWith(",") || msg.endsWith("."));
        }

        private static String messageWithTypeName(String msg, @Nullable Object typeName) {
            return msg + (msg.endsWith(" ") ? "" : ": ") + typeName;
        }

        @Nullable
        private static String nullSafeGet(@Nullable Supplier<String> messageSupplier) {
            return (messageSupplier != null ? messageSupplier.get() : null);
        }

    }

    /**
     * Miscellaneous collection utility methods.
     * Mainly for internal use within the framework.
     *
     * <p>Methods were copied from the class with the same name in Spring Framework v5.3.15
     * package "org.springframework.util", in order to support other code that was copied.
     *
     * @author Juergen Hoeller
     * @author Rob Harrop
     * @author Arjen Poutsma
     * @since 1.1.3
     */
    abstract static class CollectionUtils {

        /**
         * Return {@code true} if the supplied Collection is {@code null} or empty.
         * Otherwise, return {@code false}.
         * @param collection the Collection to check
         * @return whether the given Collection is empty
         */
        public static boolean isEmpty(@Nullable Collection<?> collection) {
            return (collection == null || collection.isEmpty());
        }

        /**
         * Return {@code true} if the supplied Map is {@code null} or empty.
         * Otherwise, return {@code false}.
         * @param map the Map to check
         * @return whether the given Map is empty
         */
        public static boolean isEmpty(@Nullable Map<?, ?> map) {
            return (map == null || map.isEmpty());
        }

    }

    /**
     * A common Spring annotation to declare that annotated elements can be {@code null} under
     * some circumstance.
     *
     * <p>Leverages JSR-305 meta-annotations to indicate nullability in Java to common
     * tools with JSR-305 support and used by Kotlin to infer nullability of Spring API.
     *
     * <p>Should be used at parameter, return value, and field level. Methods override should
     * repeat parent {@code @Nullable} annotations unless they behave differently.
     *
     * <p>Can be used in association with {@code @NonNullApi} or {@code @NonNullFields} to
     * override the default non-nullable semantic to nullable.
     *
     * <p>This file was copied as-is from Spring Framework v5.3.15 package "org.springframework.lang",
     * except for this comment, and that it is placed in a new package.
     *
     * @author Sebastien Deleuze
     * @author Juergen Hoeller
     * @since 5.0
     * @see NonNullApi
     * @see NonNullFields
     * @see NonNull
     */
    @Target({ ElementType.METHOD, ElementType.PARAMETER, ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @Nonnull(when = When.MAYBE)
    @TypeQualifierNickname
    public static @interface Nullable {
    }

    /**
     * Miscellaneous object utility methods.
     *
     * <p>Mainly for internal use within the framework.
     *
     * <p>Thanks to Alex Ruiz for contributing several enhancements to this class!
     *
     * <p>This file was copied as-is from Spring Framework v5.3.15 package "org.springframework.util",
     * except for this comment, and that it is placed in a new package.
     *
     * @author Juergen Hoeller
     * @author Keith Donald
     * @author Rod Johnson
     * @author Rob Harrop
     * @author Chris Beams
     * @author Sam Brannen
     * @since 19.03.2004
     * @see ClassUtils
     * @see CollectionUtils
     * @see StringUtils
     */
    abstract static class ObjectUtils {

        private static final int INITIAL_HASH = 7;
        private static final int MULTIPLIER = 31;

        private static final String EMPTY_STRING = "";
        private static final String NULL_STRING = "null";
        private static final String ARRAY_START = "{";
        private static final String ARRAY_END = "}";
        private static final String EMPTY_ARRAY = ARRAY_START + ARRAY_END;
        private static final String ARRAY_ELEMENT_SEPARATOR = ", ";
        private static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];


        /**
         * Return whether the given throwable is a checked exception:
         * that is, neither a RuntimeException nor an Error.
         * @param ex the throwable to check
         * @return whether the throwable is a checked exception
         * @see Exception
         * @see RuntimeException
         * @see Error
         */
        public static boolean isCheckedException(Throwable ex) {
            return !(ex instanceof RuntimeException || ex instanceof Error);
        }

        /**
         * Check whether the given exception is compatible with the specified
         * exception types, as declared in a throws clause.
         * @param ex the exception to check
         * @param declaredExceptions the exception types declared in the throws clause
         * @return whether the given exception is compatible
         */
        public static boolean isCompatibleWithThrowsClause(Throwable ex, @Nullable Class<?>... declaredExceptions) {
            if (!isCheckedException(ex)) {
                return true;
            }
            if (declaredExceptions != null) {
                for (Class<?> declaredException : declaredExceptions) {
                    if (declaredException.isInstance(ex)) {
                        return true;
                    }
                }
            }
            return false;
        }

        /**
         * Determine whether the given object is an array:
         * either an Object array or a primitive array.
         * @param obj the object to check
         */
        public static boolean isArray(@Nullable Object obj) {
            return (obj != null && obj.getClass().isArray());
        }

        /**
         * Determine whether the given array is empty:
         * i.e. {@code null} or of zero length.
         * @param array the array to check
         * @see #isEmpty(Object)
         */
        public static boolean isEmpty(@Nullable Object[] array) {
            return (array == null || array.length == 0);
        }

        /**
         * Determine whether the given object is empty.
         * <p>This method supports the following object types.
         * <ul>
         * <li>{@code Optional}: considered empty if not {@link Optional#isPresent()}</li>
         * <li>{@code Array}: considered empty if its length is zero</li>
         * <li>{@link CharSequence}: considered empty if its length is zero</li>
         * <li>{@link Collection}: delegates to {@link Collection#isEmpty()}</li>
         * <li>{@link Map}: delegates to {@link Map#isEmpty()}</li>
         * </ul>
         * <p>If the given object is non-null and not one of the aforementioned
         * supported types, this method returns {@code false}.
         * @param obj the object to check
         * @return {@code true} if the object is {@code null} or <em>empty</em>
         * @since 4.2
         * @see Optional#isPresent()
         * @see ObjectUtils#isEmpty(Object[])
         * @see StringUtils#hasLength(CharSequence)
         * @see CollectionUtils#isEmpty(Collection)
         * @see CollectionUtils#isEmpty(Map)
         */
        public static boolean isEmpty(@Nullable Object obj) {
            if (obj == null) {
                return true;
            }

            if (obj instanceof Optional) {
                return !((Optional<?>) obj).isPresent();
            }
            if (obj instanceof CharSequence) {
                return ((CharSequence) obj).length() == 0;
            }
            if (obj.getClass().isArray()) {
                return Array.getLength(obj) == 0;
            }
            if (obj instanceof Collection) {
                return ((Collection<?>) obj).isEmpty();
            }
            if (obj instanceof Map) {
                return ((Map<?, ?>) obj).isEmpty();
            }

            // else
            return false;
        }

        /**
         * Unwrap the given object which is potentially a {@link Optional}.
         * @param obj the candidate object
         * @return either the value held within the {@code Optional}, {@code null}
         * if the {@code Optional} is empty, or simply the given object as-is
         * @since 5.0
         */
        @Nullable
        public static Object unwrapOptional(@Nullable Object obj) {
            if (obj instanceof Optional) {
                Optional<?> optional = (Optional<?>) obj;
                if (!optional.isPresent()) {
                    return null;
                }
                Object result = optional.get();
                Assert.isTrue(!(result instanceof Optional), "Multi-level Optional usage not supported");
                return result;
            }
            return obj;
        }

        /**
         * Check whether the given array contains the given element.
         * @param array the array to check (may be {@code null},
         * in which case the return value will always be {@code false})
         * @param element the element to check for
         * @return whether the element has been found in the given array
         */
        public static boolean containsElement(@Nullable Object[] array, Object element) {
            if (array == null) {
                return false;
            }
            for (Object arrayEle : array) {
                if (nullSafeEquals(arrayEle, element)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Check whether the given array of enum constants contains a constant with the given name,
         * ignoring case when determining a match.
         * @param enumValues the enum values to check, typically obtained via {@code MyEnum.values()}
         * @param constant the constant name to find (must not be null or empty string)
         * @return whether the constant has been found in the given array
         */
        public static boolean containsConstant(Enum<?>[] enumValues, String constant) {
            return containsConstant(enumValues, constant, false);
        }

        /**
         * Check whether the given array of enum constants contains a constant with the given name.
         * @param enumValues the enum values to check, typically obtained via {@code MyEnum.values()}
         * @param constant the constant name to find (must not be null or empty string)
         * @param caseSensitive whether case is significant in determining a match
         * @return whether the constant has been found in the given array
         */
        public static boolean containsConstant(Enum<?>[] enumValues, String constant, boolean caseSensitive) {
            for (Enum<?> candidate : enumValues) {
                if (caseSensitive ? candidate.toString().equals(constant) :
                        candidate.toString().equalsIgnoreCase(constant)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Case insensitive alternative to {@link Enum#valueOf(Class, String)}.
         * @param <E> the concrete Enum type
         * @param enumValues the array of all Enum constants in question, usually per {@code Enum.values()}
         * @param constant the constant to get the enum value of
         * @throws IllegalArgumentException if the given constant is not found in the given array
         * of enum values. Use {@link #containsConstant(Enum[], String)} as a guard to avoid this exception.
         */
        public static <E extends Enum<?>> E caseInsensitiveValueOf(E[] enumValues, String constant) {
            for (E candidate : enumValues) {
                if (candidate.toString().equalsIgnoreCase(constant)) {
                    return candidate;
                }
            }
            throw new IllegalArgumentException("Constant [" + constant + "] does not exist in enum type " +
                    enumValues.getClass().getComponentType().getName());
        }

        /**
         * Append the given object to the given array, returning a new array
         * consisting of the input array contents plus the given object.
         * @param array the array to append to (can be {@code null})
         * @param obj the object to append
         * @return the new array (of the same component type; never {@code null})
         */
        public static <A, O extends A> A[] addObjectToArray(@Nullable A[] array, @Nullable O obj) {
            Class<?> compType = Object.class;
            if (array != null) {
                compType = array.getClass().getComponentType();
            }
            else if (obj != null) {
                compType = obj.getClass();
            }
            int newArrLength = (array != null ? array.length + 1 : 1);
            @SuppressWarnings("unchecked")
            A[] newArr = (A[]) Array.newInstance(compType, newArrLength);
            if (array != null) {
                System.arraycopy(array, 0, newArr, 0, array.length);
            }
            newArr[newArr.length - 1] = obj;
            return newArr;
        }

        /**
         * Convert the given array (which may be a primitive array) to an
         * object array (if necessary of primitive wrapper objects).
         * <p>A {@code null} source value will be converted to an
         * empty Object array.
         * @param source the (potentially primitive) array
         * @return the corresponding object array (never {@code null})
         * @throws IllegalArgumentException if the parameter is not an array
         */
        public static Object[] toObjectArray(@Nullable Object source) {
            if (source instanceof Object[]) {
                return (Object[]) source;
            }
            if (source == null) {
                return EMPTY_OBJECT_ARRAY;
            }
            if (!source.getClass().isArray()) {
                throw new IllegalArgumentException("Source is not an array: " + source);
            }
            int length = Array.getLength(source);
            if (length == 0) {
                return EMPTY_OBJECT_ARRAY;
            }
            Class<?> wrapperType = Array.get(source, 0).getClass();
            Object[] newArray = (Object[]) Array.newInstance(wrapperType, length);
            for (int i = 0; i < length; i++) {
                newArray[i] = Array.get(source, i);
            }
            return newArray;
        }


        //---------------------------------------------------------------------
        // Convenience methods for content-based equality/hash-code handling
        //---------------------------------------------------------------------

        /**
         * Determine if the given objects are equal, returning {@code true} if
         * both are {@code null} or {@code false} if only one is {@code null}.
         * <p>Compares arrays with {@code Arrays.equals}, performing an equality
         * check based on the array elements rather than the array reference.
         * @param o1 first Object to compare
         * @param o2 second Object to compare
         * @return whether the given objects are equal
         * @see Object#equals(Object)
         * @see Arrays#equals
         */
        public static boolean nullSafeEquals(@Nullable Object o1, @Nullable Object o2) {
            if (o1 == o2) {
                return true;
            }
            if (o1 == null || o2 == null) {
                return false;
            }
            if (o1.equals(o2)) {
                return true;
            }
            if (o1.getClass().isArray() && o2.getClass().isArray()) {
                return arrayEquals(o1, o2);
            }
            return false;
        }

        /**
         * Compare the given arrays with {@code Arrays.equals}, performing an equality
         * check based on the array elements rather than the array reference.
         * @param o1 first array to compare
         * @param o2 second array to compare
         * @return whether the given objects are equal
         * @see #nullSafeEquals(Object, Object)
         * @see Arrays#equals
         */
        private static boolean arrayEquals(Object o1, Object o2) {
            if (o1 instanceof Object[] && o2 instanceof Object[]) {
                return Arrays.equals((Object[]) o1, (Object[]) o2);
            }
            if (o1 instanceof boolean[] && o2 instanceof boolean[]) {
                return Arrays.equals((boolean[]) o1, (boolean[]) o2);
            }
            if (o1 instanceof byte[] && o2 instanceof byte[]) {
                return Arrays.equals((byte[]) o1, (byte[]) o2);
            }
            if (o1 instanceof char[] && o2 instanceof char[]) {
                return Arrays.equals((char[]) o1, (char[]) o2);
            }
            if (o1 instanceof double[] && o2 instanceof double[]) {
                return Arrays.equals((double[]) o1, (double[]) o2);
            }
            if (o1 instanceof float[] && o2 instanceof float[]) {
                return Arrays.equals((float[]) o1, (float[]) o2);
            }
            if (o1 instanceof int[] && o2 instanceof int[]) {
                return Arrays.equals((int[]) o1, (int[]) o2);
            }
            if (o1 instanceof long[] && o2 instanceof long[]) {
                return Arrays.equals((long[]) o1, (long[]) o2);
            }
            if (o1 instanceof short[] && o2 instanceof short[]) {
                return Arrays.equals((short[]) o1, (short[]) o2);
            }
            return false;
        }

        /**
         * Return as hash code for the given object; typically the value of
         * {@code Object#hashCode()}}. If the object is an array,
         * this method will delegate to any of the {@code nullSafeHashCode}
         * methods for arrays in this class. If the object is {@code null},
         * this method returns 0.
         * @see Object#hashCode()
         * @see #nullSafeHashCode(Object[])
         * @see #nullSafeHashCode(boolean[])
         * @see #nullSafeHashCode(byte[])
         * @see #nullSafeHashCode(char[])
         * @see #nullSafeHashCode(double[])
         * @see #nullSafeHashCode(float[])
         * @see #nullSafeHashCode(int[])
         * @see #nullSafeHashCode(long[])
         * @see #nullSafeHashCode(short[])
         */
        public static int nullSafeHashCode(@Nullable Object obj) {
            if (obj == null) {
                return 0;
            }
            if (obj.getClass().isArray()) {
                if (obj instanceof Object[]) {
                    return nullSafeHashCode((Object[]) obj);
                }
                if (obj instanceof boolean[]) {
                    return nullSafeHashCode((boolean[]) obj);
                }
                if (obj instanceof byte[]) {
                    return nullSafeHashCode((byte[]) obj);
                }
                if (obj instanceof char[]) {
                    return nullSafeHashCode((char[]) obj);
                }
                if (obj instanceof double[]) {
                    return nullSafeHashCode((double[]) obj);
                }
                if (obj instanceof float[]) {
                    return nullSafeHashCode((float[]) obj);
                }
                if (obj instanceof int[]) {
                    return nullSafeHashCode((int[]) obj);
                }
                if (obj instanceof long[]) {
                    return nullSafeHashCode((long[]) obj);
                }
                if (obj instanceof short[]) {
                    return nullSafeHashCode((short[]) obj);
                }
            }
            return obj.hashCode();
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable Object[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (Object element : array) {
                hash = MULTIPLIER * hash + nullSafeHashCode(element);
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable boolean[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (boolean element : array) {
                hash = MULTIPLIER * hash + Boolean.hashCode(element);
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable byte[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (byte element : array) {
                hash = MULTIPLIER * hash + element;
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable char[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (char element : array) {
                hash = MULTIPLIER * hash + element;
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable double[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (double element : array) {
                hash = MULTIPLIER * hash + Double.hashCode(element);
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable float[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (float element : array) {
                hash = MULTIPLIER * hash + Float.hashCode(element);
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable int[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (int element : array) {
                hash = MULTIPLIER * hash + element;
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable long[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (long element : array) {
                hash = MULTIPLIER * hash + Long.hashCode(element);
            }
            return hash;
        }

        /**
         * Return a hash code based on the contents of the specified array.
         * If {@code array} is {@code null}, this method returns 0.
         */
        public static int nullSafeHashCode(@Nullable short[] array) {
            if (array == null) {
                return 0;
            }
            int hash = INITIAL_HASH;
            for (short element : array) {
                hash = MULTIPLIER * hash + element;
            }
            return hash;
        }

        /**
         * Return the same value as {@link Boolean#hashCode(boolean)}}.
         * @deprecated as of Spring Framework 5.0, in favor of the native JDK 8 variant
         */
        @Deprecated
        public static int hashCode(boolean bool) {
            return Boolean.hashCode(bool);
        }

        /**
         * Return the same value as {@link Double#hashCode(double)}}.
         * @deprecated as of Spring Framework 5.0, in favor of the native JDK 8 variant
         */
        @Deprecated
        public static int hashCode(double dbl) {
            return Double.hashCode(dbl);
        }

        /**
         * Return the same value as {@link Float#hashCode(float)}}.
         * @deprecated as of Spring Framework 5.0, in favor of the native JDK 8 variant
         */
        @Deprecated
        public static int hashCode(float flt) {
            return Float.hashCode(flt);
        }

        /**
         * Return the same value as {@link Long#hashCode(long)}}.
         * @deprecated as of Spring Framework 5.0, in favor of the native JDK 8 variant
         */
        @Deprecated
        public static int hashCode(long lng) {
            return Long.hashCode(lng);
        }


        //---------------------------------------------------------------------
        // Convenience methods for toString output
        //---------------------------------------------------------------------

        /**
         * Return a String representation of an object's overall identity.
         * @param obj the object (may be {@code null})
         * @return the object's identity as String representation,
         * or an empty String if the object was {@code null}
         */
        public static String identityToString(@Nullable Object obj) {
            if (obj == null) {
                return EMPTY_STRING;
            }
            return obj.getClass().getName() + "@" + getIdentityHexString(obj);
        }

        /**
         * Return a hex String form of an object's identity hash code.
         * @param obj the object
         * @return the object's identity code in hex notation
         */
        public static String getIdentityHexString(Object obj) {
            return Integer.toHexString(System.identityHashCode(obj));
        }

        /**
         * Return a content-based String representation if {@code obj} is
         * not {@code null}; otherwise returns an empty String.
         * <p>Differs from {@link #nullSafeToString(Object)} in that it returns
         * an empty String rather than "null" for a {@code null} value.
         * @param obj the object to build a display String for
         * @return a display String representation of {@code obj}
         * @see #nullSafeToString(Object)
         */
        public static String getDisplayString(@Nullable Object obj) {
            if (obj == null) {
                return EMPTY_STRING;
            }
            return nullSafeToString(obj);
        }

        /**
         * Determine the class name for the given object.
         * <p>Returns a {@code "null"} String if {@code obj} is {@code null}.
         * @param obj the object to introspect (may be {@code null})
         * @return the corresponding class name
         */
        public static String nullSafeClassName(@Nullable Object obj) {
            return (obj != null ? obj.getClass().getName() : NULL_STRING);
        }

        /**
         * Return a String representation of the specified Object.
         * <p>Builds a String representation of the contents in case of an array.
         * Returns a {@code "null"} String if {@code obj} is {@code null}.
         * @param obj the object to build a String representation for
         * @return a String representation of {@code obj}
         */
        public static String nullSafeToString(@Nullable Object obj) {
            if (obj == null) {
                return NULL_STRING;
            }
            if (obj instanceof String) {
                return (String) obj;
            }
            if (obj instanceof Object[]) {
                return nullSafeToString((Object[]) obj);
            }
            if (obj instanceof boolean[]) {
                return nullSafeToString((boolean[]) obj);
            }
            if (obj instanceof byte[]) {
                return nullSafeToString((byte[]) obj);
            }
            if (obj instanceof char[]) {
                return nullSafeToString((char[]) obj);
            }
            if (obj instanceof double[]) {
                return nullSafeToString((double[]) obj);
            }
            if (obj instanceof float[]) {
                return nullSafeToString((float[]) obj);
            }
            if (obj instanceof int[]) {
                return nullSafeToString((int[]) obj);
            }
            if (obj instanceof long[]) {
                return nullSafeToString((long[]) obj);
            }
            if (obj instanceof short[]) {
                return nullSafeToString((short[]) obj);
            }
            String str = obj.toString();
            return (str != null ? str : EMPTY_STRING);
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable Object[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (Object o : array) {
                stringJoiner.add(String.valueOf(o));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable boolean[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (boolean b : array) {
                stringJoiner.add(String.valueOf(b));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable byte[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (byte b : array) {
                stringJoiner.add(String.valueOf(b));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable char[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (char c : array) {
                stringJoiner.add('\'' + String.valueOf(c) + '\'');
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable double[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (double d : array) {
                stringJoiner.add(String.valueOf(d));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable float[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (float f : array) {
                stringJoiner.add(String.valueOf(f));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable int[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (int i : array) {
                stringJoiner.add(String.valueOf(i));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable long[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (long l : array) {
                stringJoiner.add(String.valueOf(l));
            }
            return stringJoiner.toString();
        }

        /**
         * Return a String representation of the contents of the specified array.
         * <p>The String representation consists of a list of the array's elements,
         * enclosed in curly braces ({@code "{}"}). Adjacent elements are separated
         * by the characters {@code ", "} (a comma followed by a space).
         * Returns a {@code "null"} String if {@code array} is {@code null}.
         * @param array the array to build a String representation for
         * @return a String representation of {@code array}
         */
        public static String nullSafeToString(@Nullable short[] array) {
            if (array == null) {
                return NULL_STRING;
            }
            int length = array.length;
            if (length == 0) {
                return EMPTY_ARRAY;
            }
            StringJoiner stringJoiner = new StringJoiner(ARRAY_ELEMENT_SEPARATOR, ARRAY_START, ARRAY_END);
            for (short s : array) {
                stringJoiner.add(String.valueOf(s));
            }
            return stringJoiner.toString();
        }

    }

    /**
     * Miscellaneous {@link String} utility methods.
     *
     * <p>Mainly for internal use within the framework; consider
     * <a href="https://commons.apache.org/proper/commons-lang/">Apache's Commons Lang</a>
     * for a more comprehensive suite of {@code String} utilities.
     *
     * <p>This class delivers some simple functionality that should really be
     * provided by the core Java {@link String} and {@link StringBuilder}
     * classes. It also provides easy-to-use methods to convert between
     * delimited strings, such as CSV strings, and collections and arrays.
     *
     * <p>This file was copied mostly as-is from Spring Framework v5.3.15 package "org.springframework.util",
     * in order to support other code that was copied. The method "uriDecode" was removed,
     * as that depended on StreamUtils from Spring Framework, and we did not need it.
     *
     * @author Rod Johnson
     * @author Juergen Hoeller
     * @author Keith Donald
     * @author Rob Harrop
     * @author Rick Evans
     * @author Arjen Poutsma
     * @author Sam Brannen
     * @author Brian Clozel
     * @since 16 April 2001
     */
    abstract static class StringUtils {

        private static final String[] EMPTY_STRING_ARRAY = {};

        private static final String FOLDER_SEPARATOR = "/";

        private static final String WINDOWS_FOLDER_SEPARATOR = "\\";

        private static final String TOP_PATH = "..";

        private static final String CURRENT_PATH = ".";

        private static final char EXTENSION_SEPARATOR = '.';


        //---------------------------------------------------------------------
        // General convenience methods for working with Strings
        //---------------------------------------------------------------------

        /**
         * Check whether the given object (possibly a {@code String}) is empty.
         * This is effectively a shortcut for {@code !hasLength(String)}.
         * <p>This method accepts any Object as an argument, comparing it to
         * {@code null} and the empty String. As a consequence, this method
         * will never return {@code true} for a non-null non-String object.
         * <p>The Object signature is useful for general attribute handling code
         * that commonly deals with Strings but generally has to iterate over
         * Objects since attributes may e.g. be primitive value objects as well.
         * <p><b>Note: If the object is typed to {@code String} upfront, prefer
         * {@link #hasLength(String)} or {@link #hasText(String)} instead.</b>
         * @param str the candidate object (possibly a {@code String})
         * @since 3.2.1
         * @deprecated as of 5.3, in favor of {@link #hasLength(String)} and
         * {@link #hasText(String)} (or {@link ObjectUtils#isEmpty(Object)})
         */
        @Deprecated
        public static boolean isEmpty(@Nullable Object str) {
            return (str == null || "".equals(str));
        }

        /**
         * Check that the given {@code CharSequence} is neither {@code null} nor
         * of length 0.
         * <p>Note: this method returns {@code true} for a {@code CharSequence}
         * that purely consists of whitespace.
         * <p><pre class="code">
         * StringUtils.hasLength(null) = false
         * StringUtils.hasLength("") = false
         * StringUtils.hasLength(" ") = true
         * StringUtils.hasLength("Hello") = true
         * </pre>
         * @param str the {@code CharSequence} to check (may be {@code null})
         * @return {@code true} if the {@code CharSequence} is not {@code null} and has length
         * @see #hasLength(String)
         * @see #hasText(CharSequence)
         */
        public static boolean hasLength(@Nullable CharSequence str) {
            return (str != null && str.length() > 0);
        }

        /**
         * Check that the given {@code String} is neither {@code null} nor of length 0.
         * <p>Note: this method returns {@code true} for a {@code String} that
         * purely consists of whitespace.
         * @param str the {@code String} to check (may be {@code null})
         * @return {@code true} if the {@code String} is not {@code null} and has length
         * @see #hasLength(CharSequence)
         * @see #hasText(String)
         */
        public static boolean hasLength(@Nullable String str) {
            return (str != null && !str.isEmpty());
        }

        /**
         * Check whether the given {@code CharSequence} contains actual <em>text</em>.
         * <p>More specifically, this method returns {@code true} if the
         * {@code CharSequence} is not {@code null}, its length is greater than
         * 0, and it contains at least one non-whitespace character.
         * <p><pre class="code">
         * StringUtils.hasText(null) = false
         * StringUtils.hasText("") = false
         * StringUtils.hasText(" ") = false
         * StringUtils.hasText("12345") = true
         * StringUtils.hasText(" 12345 ") = true
         * </pre>
         * @param str the {@code CharSequence} to check (may be {@code null})
         * @return {@code true} if the {@code CharSequence} is not {@code null},
         * its length is greater than 0, and it does not contain whitespace only
         * @see #hasText(String)
         * @see #hasLength(CharSequence)
         * @see Character#isWhitespace
         */
        public static boolean hasText(@Nullable CharSequence str) {
            return (str != null && str.length() > 0 && containsText(str));
        }

        /**
         * Check whether the given {@code String} contains actual <em>text</em>.
         * <p>More specifically, this method returns {@code true} if the
         * {@code String} is not {@code null}, its length is greater than 0,
         * and it contains at least one non-whitespace character.
         * @param str the {@code String} to check (may be {@code null})
         * @return {@code true} if the {@code String} is not {@code null}, its
         * length is greater than 0, and it does not contain whitespace only
         * @see #hasText(CharSequence)
         * @see #hasLength(String)
         * @see Character#isWhitespace
         */
        public static boolean hasText(@Nullable String str) {
            return (str != null && !str.isEmpty() && containsText(str));
        }

        private static boolean containsText(CharSequence str) {
            int strLen = str.length();
            for (int i = 0; i < strLen; i++) {
                if (!Character.isWhitespace(str.charAt(i))) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Check whether the given {@code CharSequence} contains any whitespace characters.
         * @param str the {@code CharSequence} to check (may be {@code null})
         * @return {@code true} if the {@code CharSequence} is not empty and
         * contains at least 1 whitespace character
         * @see Character#isWhitespace
         */
        public static boolean containsWhitespace(@Nullable CharSequence str) {
            if (!hasLength(str)) {
                return false;
            }

            int strLen = str.length();
            for (int i = 0; i < strLen; i++) {
                if (Character.isWhitespace(str.charAt(i))) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Check whether the given {@code String} contains any whitespace characters.
         * @param str the {@code String} to check (may be {@code null})
         * @return {@code true} if the {@code String} is not empty and
         * contains at least 1 whitespace character
         * @see #containsWhitespace(CharSequence)
         */
        public static boolean containsWhitespace(@Nullable String str) {
            return containsWhitespace((CharSequence) str);
        }

        /**
         * Trim leading and trailing whitespace from the given {@code String}.
         * @param str the {@code String} to check
         * @return the trimmed {@code String}
         * @see Character#isWhitespace
         */
        public static String trimWhitespace(String str) {
            if (!hasLength(str)) {
                return str;
            }

            int beginIndex = 0;
            int endIndex = str.length() - 1;

            while (beginIndex <= endIndex && Character.isWhitespace(str.charAt(beginIndex))) {
                beginIndex++;
            }

            while (endIndex > beginIndex && Character.isWhitespace(str.charAt(endIndex))) {
                endIndex--;
            }

            return str.substring(beginIndex, endIndex + 1);
        }

        /**
         * Trim <i>all</i> whitespace from the given {@code String}:
         * leading, trailing, and in between characters.
         * @param str the {@code String} to check
         * @return the trimmed {@code String}
         * @see Character#isWhitespace
         */
        public static String trimAllWhitespace(String str) {
            if (!hasLength(str)) {
                return str;
            }

            int len = str.length();
            StringBuilder sb = new StringBuilder(str.length());
            for (int i = 0; i < len; i++) {
                char c = str.charAt(i);
                if (!Character.isWhitespace(c)) {
                    sb.append(c);
                }
            }
            return sb.toString();
        }

        /**
         * Trim leading whitespace from the given {@code String}.
         * @param str the {@code String} to check
         * @return the trimmed {@code String}
         * @see Character#isWhitespace
         */
        public static String trimLeadingWhitespace(String str) {
            if (!hasLength(str)) {
                return str;
            }

            int beginIdx = 0;
            while (beginIdx < str.length() && Character.isWhitespace(str.charAt(beginIdx))) {
                beginIdx++;
            }
            return str.substring(beginIdx);
        }

        /**
         * Trim trailing whitespace from the given {@code String}.
         * @param str the {@code String} to check
         * @return the trimmed {@code String}
         * @see Character#isWhitespace
         */
        public static String trimTrailingWhitespace(String str) {
            if (!hasLength(str)) {
                return str;
            }

            int endIdx = str.length() - 1;
            while (endIdx >= 0 && Character.isWhitespace(str.charAt(endIdx))) {
                endIdx--;
            }
            return str.substring(0, endIdx + 1);
        }

        /**
         * Trim all occurrences of the supplied leading character from the given {@code String}.
         * @param str the {@code String} to check
         * @param leadingCharacter the leading character to be trimmed
         * @return the trimmed {@code String}
         */
        public static String trimLeadingCharacter(String str, char leadingCharacter) {
            if (!hasLength(str)) {
                return str;
            }

            int beginIdx = 0;
            while (beginIdx < str.length() && leadingCharacter == str.charAt(beginIdx)) {
                beginIdx++;
            }
            return str.substring(beginIdx);
        }

        /**
         * Trim all occurrences of the supplied trailing character from the given {@code String}.
         * @param str the {@code String} to check
         * @param trailingCharacter the trailing character to be trimmed
         * @return the trimmed {@code String}
         */
        public static String trimTrailingCharacter(String str, char trailingCharacter) {
            if (!hasLength(str)) {
                return str;
            }

            int endIdx = str.length() - 1;
            while (endIdx >= 0 && trailingCharacter == str.charAt(endIdx)) {
                endIdx--;
            }
            return str.substring(0, endIdx + 1);
        }

        /**
         * Test if the given {@code String} matches the given single character.
         * @param str the {@code String} to check
         * @param singleCharacter the character to compare to
         * @since 5.2.9
         */
        public static boolean matchesCharacter(@Nullable String str, char singleCharacter) {
            return (str != null && str.length() == 1 && str.charAt(0) == singleCharacter);
        }

        /**
         * Test if the given {@code String} starts with the specified prefix,
         * ignoring upper/lower case.
         * @param str the {@code String} to check
         * @param prefix the prefix to look for
         * @see String#startsWith
         */
        public static boolean startsWithIgnoreCase(@Nullable String str, @Nullable String prefix) {
            return (str != null && prefix != null && str.length() >= prefix.length() &&
                    str.regionMatches(true, 0, prefix, 0, prefix.length()));
        }

        /**
         * Test if the given {@code String} ends with the specified suffix,
         * ignoring upper/lower case.
         * @param str the {@code String} to check
         * @param suffix the suffix to look for
         * @see String#endsWith
         */
        public static boolean endsWithIgnoreCase(@Nullable String str, @Nullable String suffix) {
            return (str != null && suffix != null && str.length() >= suffix.length() &&
                    str.regionMatches(true, str.length() - suffix.length(), suffix, 0, suffix.length()));
        }

        /**
         * Test whether the given string matches the given substring
         * at the given index.
         * @param str the original string (or StringBuilder)
         * @param index the index in the original string to start matching against
         * @param substring the substring to match at the given index
         */
        public static boolean substringMatch(CharSequence str, int index, CharSequence substring) {
            if (index + substring.length() > str.length()) {
                return false;
            }
            for (int i = 0; i < substring.length(); i++) {
                if (str.charAt(index + i) != substring.charAt(i)) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Count the occurrences of the substring {@code sub} in string {@code str}.
         * @param str string to search in
         * @param sub string to search for
         */
        public static int countOccurrencesOf(String str, String sub) {
            if (!hasLength(str) || !hasLength(sub)) {
                return 0;
            }

            int count = 0;
            int pos = 0;
            int idx;
            while ((idx = str.indexOf(sub, pos)) != -1) {
                ++count;
                pos = idx + sub.length();
            }
            return count;
        }

        /**
         * Replace all occurrences of a substring within a string with another string.
         * @param inString {@code String} to examine
         * @param oldPattern {@code String} to replace
         * @param newPattern {@code String} to insert
         * @return a {@code String} with the replacements
         */
        public static String replace(String inString, String oldPattern, @Nullable String newPattern) {
            if (!hasLength(inString) || !hasLength(oldPattern) || newPattern == null) {
                return inString;
            }
            int index = inString.indexOf(oldPattern);
            if (index == -1) {
                // no occurrence -> can return input as-is
                return inString;
            }

            int capacity = inString.length();
            if (newPattern.length() > oldPattern.length()) {
                capacity += 16;
            }
            StringBuilder sb = new StringBuilder(capacity);

            int pos = 0;  // our position in the old string
            int patLen = oldPattern.length();
            while (index >= 0) {
                sb.append(inString, pos, index);
                sb.append(newPattern);
                pos = index + patLen;
                index = inString.indexOf(oldPattern, pos);
            }

            // append any characters to the right of a match
            sb.append(inString, pos, inString.length());
            return sb.toString();
        }

        /**
         * Delete all occurrences of the given substring.
         * @param inString the original {@code String}
         * @param pattern the pattern to delete all occurrences of
         * @return the resulting {@code String}
         */
        public static String delete(String inString, String pattern) {
            return replace(inString, pattern, "");
        }

        /**
         * Delete any character in a given {@code String}.
         * @param inString the original {@code String}
         * @param charsToDelete a set of characters to delete.
         * E.g. "az\n" will delete 'a's, 'z's and new lines.
         * @return the resulting {@code String}
         */
        public static String deleteAny(String inString, @Nullable String charsToDelete) {
            if (!hasLength(inString) || !hasLength(charsToDelete)) {
                return inString;
            }

            int lastCharIndex = 0;
            char[] result = new char[inString.length()];
            for (int i = 0; i < inString.length(); i++) {
                char c = inString.charAt(i);
                if (charsToDelete.indexOf(c) == -1) {
                    result[lastCharIndex++] = c;
                }
            }
            if (lastCharIndex == inString.length()) {
                return inString;
            }
            return new String(result, 0, lastCharIndex);
        }

        //---------------------------------------------------------------------
        // Convenience methods for working with formatted Strings
        //---------------------------------------------------------------------

        /**
         * Quote the given {@code String} with single quotes.
         * @param str the input {@code String} (e.g. "myString")
         * @return the quoted {@code String} (e.g. "'myString'"),
         * or {@code null} if the input was {@code null}
         */
        @Nullable
        public static String quote(@Nullable String str) {
            return (str != null ? "'" + str + "'" : null);
        }

        /**
         * Turn the given Object into a {@code String} with single quotes
         * if it is a {@code String}; keeping the Object as-is else.
         * @param obj the input Object (e.g. "myString")
         * @return the quoted {@code String} (e.g. "'myString'"),
         * or the input object as-is if not a {@code String}
         */
        @Nullable
        public static Object quoteIfString(@Nullable Object obj) {
            return (obj instanceof String ? quote((String) obj) : obj);
        }

        /**
         * Unqualify a string qualified by a '.' dot character. For example,
         * "this.name.is.qualified", returns "qualified".
         * @param qualifiedName the qualified name
         */
        public static String unqualify(String qualifiedName) {
            return unqualify(qualifiedName, '.');
        }

        /**
         * Unqualify a string qualified by a separator character. For example,
         * "this:name:is:qualified" returns "qualified" if using a ':' separator.
         * @param qualifiedName the qualified name
         * @param separator the separator
         */
        public static String unqualify(String qualifiedName, char separator) {
            return qualifiedName.substring(qualifiedName.lastIndexOf(separator) + 1);
        }

        /**
         * Capitalize a {@code String}, changing the first letter to
         * upper case as per {@link Character#toUpperCase(char)}.
         * No other letters are changed.
         * @param str the {@code String} to capitalize
         * @return the capitalized {@code String}
         */
        public static String capitalize(String str) {
            return changeFirstCharacterCase(str, true);
        }

        /**
         * Uncapitalize a {@code String}, changing the first letter to
         * lower case as per {@link Character#toLowerCase(char)}.
         * No other letters are changed.
         * @param str the {@code String} to uncapitalize
         * @return the uncapitalized {@code String}
         */
        public static String uncapitalize(String str) {
            return changeFirstCharacterCase(str, false);
        }

        private static String changeFirstCharacterCase(String str, boolean capitalize) {
            if (!hasLength(str)) {
                return str;
            }

            char baseChar = str.charAt(0);
            char updatedChar;
            if (capitalize) {
                updatedChar = Character.toUpperCase(baseChar);
            }
            else {
                updatedChar = Character.toLowerCase(baseChar);
            }
            if (baseChar == updatedChar) {
                return str;
            }

            char[] chars = str.toCharArray();
            chars[0] = updatedChar;
            return new String(chars);
        }

        /**
         * Extract the filename from the given Java resource path,
         * e.g. {@code "mypath/myfile.txt" &rarr; "myfile.txt"}.
         * @param path the file path (may be {@code null})
         * @return the extracted filename, or {@code null} if none
         */
        @Nullable
        public static String getFilename(@Nullable String path) {
            if (path == null) {
                return null;
            }

            int separatorIndex = path.lastIndexOf(FOLDER_SEPARATOR);
            return (separatorIndex != -1 ? path.substring(separatorIndex + 1) : path);
        }

        /**
         * Extract the filename extension from the given Java resource path,
         * e.g. "mypath/myfile.txt" &rarr; "txt".
         * @param path the file path (may be {@code null})
         * @return the extracted filename extension, or {@code null} if none
         */
        @Nullable
        public static String getFilenameExtension(@Nullable String path) {
            if (path == null) {
                return null;
            }

            int extIndex = path.lastIndexOf(EXTENSION_SEPARATOR);
            if (extIndex == -1) {
                return null;
            }

            int folderIndex = path.lastIndexOf(FOLDER_SEPARATOR);
            if (folderIndex > extIndex) {
                return null;
            }

            return path.substring(extIndex + 1);
        }

        /**
         * Strip the filename extension from the given Java resource path,
         * e.g. "mypath/myfile.txt" &rarr; "mypath/myfile".
         * @param path the file path
         * @return the path with stripped filename extension
         */
        public static String stripFilenameExtension(String path) {
            int extIndex = path.lastIndexOf(EXTENSION_SEPARATOR);
            if (extIndex == -1) {
                return path;
            }

            int folderIndex = path.lastIndexOf(FOLDER_SEPARATOR);
            if (folderIndex > extIndex) {
                return path;
            }

            return path.substring(0, extIndex);
        }

        /**
         * Apply the given relative path to the given Java resource path,
         * assuming standard Java folder separation (i.e. "/" separators).
         * @param path the path to start from (usually a full file path)
         * @param relativePath the relative path to apply
         * (relative to the full file path above)
         * @return the full file path that results from applying the relative path
         */
        public static String applyRelativePath(String path, String relativePath) {
            int separatorIndex = path.lastIndexOf(FOLDER_SEPARATOR);
            if (separatorIndex != -1) {
                String newPath = path.substring(0, separatorIndex);
                if (!relativePath.startsWith(FOLDER_SEPARATOR)) {
                    newPath += FOLDER_SEPARATOR;
                }
                return newPath + relativePath;
            }
            else {
                return relativePath;
            }
        }

        /**
         * Normalize the path by suppressing sequences like "path/.." and
         * inner simple dots.
         * <p>The result is convenient for path comparison. For other uses,
         * notice that Windows separators ("\") are replaced by simple slashes.
         * <p><strong>NOTE</strong> that {@code cleanPath} should not be depended
         * upon in a security context. Other mechanisms should be used to prevent
         * path-traversal issues.
         * @param path the original path
         * @return the normalized path
         */
        public static String cleanPath(String path) {
            if (!hasLength(path)) {
                return path;
            }

            String normalizedPath = replace(path, WINDOWS_FOLDER_SEPARATOR, FOLDER_SEPARATOR);
            String pathToUse = normalizedPath;

            // Shortcut if there is no work to do
            if (pathToUse.indexOf('.') == -1) {
                return pathToUse;
            }

            // Strip prefix from path to analyze, to not treat it as part of the
            // first path element. This is necessary to correctly parse paths like
            // "file:core/../core/io/Resource.class", where the ".." should just
            // strip the first "core" directory while keeping the "file:" prefix.
            int prefixIndex = pathToUse.indexOf(':');
            String prefix = "";
            if (prefixIndex != -1) {
                prefix = pathToUse.substring(0, prefixIndex + 1);
                if (prefix.contains(FOLDER_SEPARATOR)) {
                    prefix = "";
                }
                else {
                    pathToUse = pathToUse.substring(prefixIndex + 1);
                }
            }
            if (pathToUse.startsWith(FOLDER_SEPARATOR)) {
                prefix = prefix + FOLDER_SEPARATOR;
                pathToUse = pathToUse.substring(1);
            }

            String[] pathArray = delimitedListToStringArray(pathToUse, FOLDER_SEPARATOR);
            // we never require more elements than pathArray and in the common case the same number
            Deque<String> pathElements = new ArrayDeque<>(pathArray.length);
            int tops = 0;

            for (int i = pathArray.length - 1; i >= 0; i--) {
                String element = pathArray[i];
                if (CURRENT_PATH.equals(element)) {
                    // Points to current directory - drop it.
                }
                else if (TOP_PATH.equals(element)) {
                    // Registering top path found.
                    tops++;
                }
                else {
                    if (tops > 0) {
                        // Merging path element with element corresponding to top path.
                        tops--;
                    }
                    else {
                        // Normal path element found.
                        pathElements.addFirst(element);
                    }
                }
            }

            // All path elements stayed the same - shortcut
            if (pathArray.length == pathElements.size()) {
                return normalizedPath;
            }
            // Remaining top paths need to be retained.
            for (int i = 0; i < tops; i++) {
                pathElements.addFirst(TOP_PATH);
            }
            // If nothing else left, at least explicitly point to current path.
            if (pathElements.size() == 1 && pathElements.getLast().isEmpty() && !prefix.endsWith(FOLDER_SEPARATOR)) {
                pathElements.addFirst(CURRENT_PATH);
            }

            final String joined = collectionToDelimitedString(pathElements, FOLDER_SEPARATOR);
            // avoid string concatenation with empty prefix
            return prefix.isEmpty() ? joined : prefix + joined;
        }

        /**
         * Compare two paths after normalization of them.
         * @param path1 first path for comparison
         * @param path2 second path for comparison
         * @return whether the two paths are equivalent after normalization
         */
        public static boolean pathEquals(String path1, String path2) {
            return cleanPath(path1).equals(cleanPath(path2));
        }

        /**
         * Parse the given {@code String} value into a {@link Locale}, accepting
         * the {@link Locale#toString} format as well as BCP 47 language tags.
         * @param localeValue the locale value: following either {@code Locale's}
         * {@code toString()} format ("en", "en_UK", etc), also accepting spaces as
         * separators (as an alternative to underscores), or BCP 47 (e.g. "en-UK")
         * as specified by {@link Locale#forLanguageTag} on Java 7+
         * @return a corresponding {@code Locale} instance, or {@code null} if none
         * @throws IllegalArgumentException in case of an invalid locale specification
         * @since 5.0.4
         * @see #parseLocaleString
         * @see Locale#forLanguageTag
         */
        @Nullable
        public static Locale parseLocale(String localeValue) {
            String[] tokens = tokenizeLocaleSource(localeValue);
            if (tokens.length == 1) {
                validateLocalePart(localeValue);
                Locale resolved = Locale.forLanguageTag(localeValue);
                if (resolved.getLanguage().length() > 0) {
                    return resolved;
                }
            }
            return parseLocaleTokens(localeValue, tokens);
        }

        /**
         * Parse the given {@code String} representation into a {@link Locale}.
         * <p>For many parsing scenarios, this is an inverse operation of
         * {@link Locale#toString Locale's toString}, in a lenient sense.
         * This method does not aim for strict {@code Locale} design compliance;
         * it is rather specifically tailored for typical Spring parsing needs.
         * <p><b>Note: This delegate does not accept the BCP 47 language tag format.
         * Please use {@link #parseLocale} for lenient parsing of both formats.</b>
         * @param localeString the locale {@code String}: following {@code Locale's}
         * {@code toString()} format ("en", "en_UK", etc), also accepting spaces as
         * separators (as an alternative to underscores)
         * @return a corresponding {@code Locale} instance, or {@code null} if none
         * @throws IllegalArgumentException in case of an invalid locale specification
         */
        @Nullable
        public static Locale parseLocaleString(String localeString) {
            return parseLocaleTokens(localeString, tokenizeLocaleSource(localeString));
        }

        private static String[] tokenizeLocaleSource(String localeSource) {
            return tokenizeToStringArray(localeSource, "_ ", false, false);
        }

        @Nullable
        private static Locale parseLocaleTokens(String localeString, String[] tokens) {
            String language = (tokens.length > 0 ? tokens[0] : "");
            String country = (tokens.length > 1 ? tokens[1] : "");
            validateLocalePart(language);
            validateLocalePart(country);

            String variant = "";
            if (tokens.length > 2) {
                // There is definitely a variant, and it is everything after the country
                // code sans the separator between the country code and the variant.
                int endIndexOfCountryCode = localeString.indexOf(country, language.length()) + country.length();
                // Strip off any leading '_' and whitespace, what's left is the variant.
                variant = trimLeadingWhitespace(localeString.substring(endIndexOfCountryCode));
                if (variant.startsWith("_")) {
                    variant = trimLeadingCharacter(variant, '_');
                }
            }

            if (variant.isEmpty() && country.startsWith("#")) {
                variant = country;
                country = "";
            }

            return (language.length() > 0 ? new Locale(language, country, variant) : null);
        }

        private static void validateLocalePart(String localePart) {
            for (int i = 0; i < localePart.length(); i++) {
                char ch = localePart.charAt(i);
                if (ch != ' ' && ch != '_' && ch != '-' && ch != '#' && !Character.isLetterOrDigit(ch)) {
                    throw new IllegalArgumentException(
                            "Locale part \"" + localePart + "\" contains invalid characters");
                }
            }
        }

        /**
         * Determine the RFC 3066 compliant language tag,
         * as used for the HTTP "Accept-Language" header.
         * @param locale the Locale to transform to a language tag
         * @return the RFC 3066 compliant language tag as {@code String}
         * @deprecated as of 5.0.4, in favor of {@link Locale#toLanguageTag()}
         */
        @Deprecated
        public static String toLanguageTag(Locale locale) {
            return locale.getLanguage() + (hasText(locale.getCountry()) ? "-" + locale.getCountry() : "");
        }

        /**
         * Parse the given {@code timeZoneString} value into a {@link TimeZone}.
         * @param timeZoneString the time zone {@code String}, following {@link TimeZone#getTimeZone(String)}
         * but throwing {@link IllegalArgumentException} in case of an invalid time zone specification
         * @return a corresponding {@link TimeZone} instance
         * @throws IllegalArgumentException in case of an invalid time zone specification
         */
        public static TimeZone parseTimeZoneString(String timeZoneString) {
            TimeZone timeZone = TimeZone.getTimeZone(timeZoneString);
            if ("GMT".equals(timeZone.getID()) && !timeZoneString.startsWith("GMT")) {
                // We don't want that GMT fallback...
                throw new IllegalArgumentException("Invalid time zone specification '" + timeZoneString + "'");
            }
            return timeZone;
        }


        //---------------------------------------------------------------------
        // Convenience methods for working with String arrays
        //---------------------------------------------------------------------

        /**
         * Copy the given {@link Collection} into a {@code String} array.
         * <p>The {@code Collection} must contain {@code String} elements only.
         * @param collection the {@code Collection} to copy
         * (potentially {@code null} or empty)
         * @return the resulting {@code String} array
         */
        public static String[] toStringArray(@Nullable Collection<String> collection) {
            return (!CollectionUtils.isEmpty(collection) ? collection.toArray(EMPTY_STRING_ARRAY) : EMPTY_STRING_ARRAY);
        }

        /**
         * Copy the given {@link Enumeration} into a {@code String} array.
         * <p>The {@code Enumeration} must contain {@code String} elements only.
         * @param enumeration the {@code Enumeration} to copy
         * (potentially {@code null} or empty)
         * @return the resulting {@code String} array
         */
        public static String[] toStringArray(@Nullable Enumeration<String> enumeration) {
            return (enumeration != null ? toStringArray(Collections.list(enumeration)) : EMPTY_STRING_ARRAY);
        }

        /**
         * Append the given {@code String} to the given {@code String} array,
         * returning a new array consisting of the input array contents plus
         * the given {@code String}.
         * @param array the array to append to (can be {@code null})
         * @param str the {@code String} to append
         * @return the new array (never {@code null})
         */
        public static String[] addStringToArray(@Nullable String[] array, String str) {
            if (ObjectUtils.isEmpty(array)) {
                return new String[] {str};
            }

            String[] newArr = new String[array.length + 1];
            System.arraycopy(array, 0, newArr, 0, array.length);
            newArr[array.length] = str;
            return newArr;
        }

        /**
         * Concatenate the given {@code String} arrays into one,
         * with overlapping array elements included twice.
         * <p>The order of elements in the original arrays is preserved.
         * @param array1 the first array (can be {@code null})
         * @param array2 the second array (can be {@code null})
         * @return the new array ({@code null} if both given arrays were {@code null})
         */
        @Nullable
        public static String[] concatenateStringArrays(@Nullable String[] array1, @Nullable String[] array2) {
            if (ObjectUtils.isEmpty(array1)) {
                return array2;
            }
            if (ObjectUtils.isEmpty(array2)) {
                return array1;
            }

            String[] newArr = new String[array1.length + array2.length];
            System.arraycopy(array1, 0, newArr, 0, array1.length);
            System.arraycopy(array2, 0, newArr, array1.length, array2.length);
            return newArr;
        }

        /**
         * Merge the given {@code String} arrays into one, with overlapping
         * array elements only included once.
         * <p>The order of elements in the original arrays is preserved
         * (with the exception of overlapping elements, which are only
         * included on their first occurrence).
         * @param array1 the first array (can be {@code null})
         * @param array2 the second array (can be {@code null})
         * @return the new array ({@code null} if both given arrays were {@code null})
         * @deprecated as of 4.3.15, in favor of manual merging via {@link LinkedHashSet}
         * (with every entry included at most once, even entries within the first array)
         */
        @Deprecated
        @Nullable
        public static String[] mergeStringArrays(@Nullable String[] array1, @Nullable String[] array2) {
            if (ObjectUtils.isEmpty(array1)) {
                return array2;
            }
            if (ObjectUtils.isEmpty(array2)) {
                return array1;
            }

            List<String> result = new ArrayList<>(Arrays.asList(array1));
            for (String str : array2) {
                if (!result.contains(str)) {
                    result.add(str);
                }
            }
            return toStringArray(result);
        }

        /**
         * Sort the given {@code String} array if necessary.
         * @param array the original array (potentially empty)
         * @return the array in sorted form (never {@code null})
         */
        public static String[] sortStringArray(String[] array) {
            if (ObjectUtils.isEmpty(array)) {
                return array;
            }

            Arrays.sort(array);
            return array;
        }

        /**
         * Trim the elements of the given {@code String} array, calling
         * {@code String.trim()} on each non-null element.
         * @param array the original {@code String} array (potentially empty)
         * @return the resulting array (of the same size) with trimmed elements
         */
        public static String[] trimArrayElements(String[] array) {
            if (ObjectUtils.isEmpty(array)) {
                return array;
            }

            String[] result = new String[array.length];
            for (int i = 0; i < array.length; i++) {
                String element = array[i];
                result[i] = (element != null ? element.trim() : null);
            }
            return result;
        }

        /**
         * Remove duplicate strings from the given array.
         * <p>As of 4.2, it preserves the original order, as it uses a {@link LinkedHashSet}.
         * @param array the {@code String} array (potentially empty)
         * @return an array without duplicates, in natural sort order
         */
        public static String[] removeDuplicateStrings(String[] array) {
            if (ObjectUtils.isEmpty(array)) {
                return array;
            }

            Set<String> set = new LinkedHashSet<>(Arrays.asList(array));
            return toStringArray(set);
        }

        /**
         * Split a {@code String} at the first occurrence of the delimiter.
         * Does not include the delimiter in the result.
         * @param toSplit the string to split (potentially {@code null} or empty)
         * @param delimiter to split the string up with (potentially {@code null} or empty)
         * @return a two element array with index 0 being before the delimiter, and
         * index 1 being after the delimiter (neither element includes the delimiter);
         * or {@code null} if the delimiter wasn't found in the given input {@code String}
         */
        @Nullable
        public static String[] split(@Nullable String toSplit, @Nullable String delimiter) {
            if (!hasLength(toSplit) || !hasLength(delimiter)) {
                return null;
            }
            int offset = toSplit.indexOf(delimiter);
            if (offset < 0) {
                return null;
            }

            String beforeDelimiter = toSplit.substring(0, offset);
            String afterDelimiter = toSplit.substring(offset + delimiter.length());
            return new String[] {beforeDelimiter, afterDelimiter};
        }

        /**
         * Take an array of strings and split each element based on the given delimiter.
         * A {@code Properties} instance is then generated, with the left of the delimiter
         * providing the key, and the right of the delimiter providing the value.
         * <p>Will trim both the key and value before adding them to the {@code Properties}.
         * @param array the array to process
         * @param delimiter to split each element using (typically the equals symbol)
         * @return a {@code Properties} instance representing the array contents,
         * or {@code null} if the array to process was {@code null} or empty
         */
        @Nullable
        public static Properties splitArrayElementsIntoProperties(String[] array, String delimiter) {
            return splitArrayElementsIntoProperties(array, delimiter, null);
        }

        /**
         * Take an array of strings and split each element based on the given delimiter.
         * A {@code Properties} instance is then generated, with the left of the
         * delimiter providing the key, and the right of the delimiter providing the value.
         * <p>Will trim both the key and value before adding them to the
         * {@code Properties} instance.
         * @param array the array to process
         * @param delimiter to split each element using (typically the equals symbol)
         * @param charsToDelete one or more characters to remove from each element
         * prior to attempting the split operation (typically the quotation mark
         * symbol), or {@code null} if no removal should occur
         * @return a {@code Properties} instance representing the array contents,
         * or {@code null} if the array to process was {@code null} or empty
         */
        @Nullable
        public static Properties splitArrayElementsIntoProperties(
                String[] array, String delimiter, @Nullable String charsToDelete) {

            if (ObjectUtils.isEmpty(array)) {
                return null;
            }

            Properties result = new Properties();
            for (String element : array) {
                if (charsToDelete != null) {
                    element = deleteAny(element, charsToDelete);
                }
                String[] splittedElement = split(element, delimiter);
                if (splittedElement == null) {
                    continue;
                }
                result.setProperty(splittedElement[0].trim(), splittedElement[1].trim());
            }
            return result;
        }

        /**
         * Tokenize the given {@code String} into a {@code String} array via a
         * {@link StringTokenizer}.
         * <p>Trims tokens and omits empty tokens.
         * <p>The given {@code delimiters} string can consist of any number of
         * delimiter characters. Each of those characters can be used to separate
         * tokens. A delimiter is always a single character; for multi-character
         * delimiters, consider using {@link #delimitedListToStringArray}.
         * @param str the {@code String} to tokenize (potentially {@code null} or empty)
         * @param delimiters the delimiter characters, assembled as a {@code String}
         * (each of the characters is individually considered as a delimiter)
         * @return an array of the tokens
         * @see StringTokenizer
         * @see String#trim()
         * @see #delimitedListToStringArray
         */
        public static String[] tokenizeToStringArray(@Nullable String str, String delimiters) {
            return tokenizeToStringArray(str, delimiters, true, true);
        }

        /**
         * Tokenize the given {@code String} into a {@code String} array via a
         * {@link StringTokenizer}.
         * <p>The given {@code delimiters} string can consist of any number of
         * delimiter characters. Each of those characters can be used to separate
         * tokens. A delimiter is always a single character; for multi-character
         * delimiters, consider using {@link #delimitedListToStringArray}.
         * @param str the {@code String} to tokenize (potentially {@code null} or empty)
         * @param delimiters the delimiter characters, assembled as a {@code String}
         * (each of the characters is individually considered as a delimiter)
         * @param trimTokens trim the tokens via {@link String#trim()}
         * @param ignoreEmptyTokens omit empty tokens from the result array
         * (only applies to tokens that are empty after trimming; StringTokenizer
         * will not consider subsequent delimiters as token in the first place).
         * @return an array of the tokens
         * @see StringTokenizer
         * @see String#trim()
         * @see #delimitedListToStringArray
         */
        public static String[] tokenizeToStringArray(
                @Nullable String str, String delimiters, boolean trimTokens, boolean ignoreEmptyTokens) {

            if (str == null) {
                return EMPTY_STRING_ARRAY;
            }

            StringTokenizer st = new StringTokenizer(str, delimiters);
            List<String> tokens = new ArrayList<>();
            while (st.hasMoreTokens()) {
                String token = st.nextToken();
                if (trimTokens) {
                    token = token.trim();
                }
                if (!ignoreEmptyTokens || token.length() > 0) {
                    tokens.add(token);
                }
            }
            return toStringArray(tokens);
        }

        /**
         * Take a {@code String} that is a delimited list and convert it into a
         * {@code String} array.
         * <p>A single {@code delimiter} may consist of more than one character,
         * but it will still be considered as a single delimiter string, rather
         * than as bunch of potential delimiter characters, in contrast to
         * {@link #tokenizeToStringArray}.
         * @param str the input {@code String} (potentially {@code null} or empty)
         * @param delimiter the delimiter between elements (this is a single delimiter,
         * rather than a bunch individual delimiter characters)
         * @return an array of the tokens in the list
         * @see #tokenizeToStringArray
         */
        public static String[] delimitedListToStringArray(@Nullable String str, @Nullable String delimiter) {
            return delimitedListToStringArray(str, delimiter, null);
        }

        /**
         * Take a {@code String} that is a delimited list and convert it into
         * a {@code String} array.
         * <p>A single {@code delimiter} may consist of more than one character,
         * but it will still be considered as a single delimiter string, rather
         * than as bunch of potential delimiter characters, in contrast to
         * {@link #tokenizeToStringArray}.
         * @param str the input {@code String} (potentially {@code null} or empty)
         * @param delimiter the delimiter between elements (this is a single delimiter,
         * rather than a bunch individual delimiter characters)
         * @param charsToDelete a set of characters to delete; useful for deleting unwanted
         * line breaks: e.g. "\r\n\f" will delete all new lines and line feeds in a {@code String}
         * @return an array of the tokens in the list
         * @see #tokenizeToStringArray
         */
        public static String[] delimitedListToStringArray(
                @Nullable String str, @Nullable String delimiter, @Nullable String charsToDelete) {

            if (str == null) {
                return EMPTY_STRING_ARRAY;
            }
            if (delimiter == null) {
                return new String[] {str};
            }

            List<String> result = new ArrayList<>();
            if (delimiter.isEmpty()) {
                for (int i = 0; i < str.length(); i++) {
                    result.add(deleteAny(str.substring(i, i + 1), charsToDelete));
                }
            }
            else {
                int pos = 0;
                int delPos;
                while ((delPos = str.indexOf(delimiter, pos)) != -1) {
                    result.add(deleteAny(str.substring(pos, delPos), charsToDelete));
                    pos = delPos + delimiter.length();
                }
                if (str.length() > 0 && pos <= str.length()) {
                    // Add rest of String, but not in case of empty input.
                    result.add(deleteAny(str.substring(pos), charsToDelete));
                }
            }
            return toStringArray(result);
        }

        /**
         * Convert a comma delimited list (e.g., a row from a CSV file) into an
         * array of strings.
         * @param str the input {@code String} (potentially {@code null} or empty)
         * @return an array of strings, or the empty array in case of empty input
         */
        public static String[] commaDelimitedListToStringArray(@Nullable String str) {
            return delimitedListToStringArray(str, ",");
        }

        /**
         * Convert a comma delimited list (e.g., a row from a CSV file) into a set.
         * <p>Note that this will suppress duplicates, and as of 4.2, the elements in
         * the returned set will preserve the original order in a {@link LinkedHashSet}.
         * @param str the input {@code String} (potentially {@code null} or empty)
         * @return a set of {@code String} entries in the list
         * @see #removeDuplicateStrings(String[])
         */
        public static Set<String> commaDelimitedListToSet(@Nullable String str) {
            String[] tokens = commaDelimitedListToStringArray(str);
            return new LinkedHashSet<>(Arrays.asList(tokens));
        }

        /**
         * Convert a {@link Collection} to a delimited {@code String} (e.g. CSV).
         * <p>Useful for {@code toString()} implementations.
         * @param coll the {@code Collection} to convert (potentially {@code null} or empty)
         * @param delim the delimiter to use (typically a ",")
         * @param prefix the {@code String} to start each element with
         * @param suffix the {@code String} to end each element with
         * @return the delimited {@code String}
         */
        public static String collectionToDelimitedString(
                @Nullable Collection<?> coll, String delim, String prefix, String suffix) {

            if (CollectionUtils.isEmpty(coll)) {
                return "";
            }

            int totalLength = coll.size() * (prefix.length() + suffix.length()) + (coll.size() - 1) * delim.length();
            for (Object element : coll) {
                totalLength += String.valueOf(element).length();
            }

            StringBuilder sb = new StringBuilder(totalLength);
            Iterator<?> it = coll.iterator();
            while (it.hasNext()) {
                sb.append(prefix).append(it.next()).append(suffix);
                if (it.hasNext()) {
                    sb.append(delim);
                }
            }
            return sb.toString();
        }

        /**
         * Convert a {@code Collection} into a delimited {@code String} (e.g. CSV).
         * <p>Useful for {@code toString()} implementations.
         * @param coll the {@code Collection} to convert (potentially {@code null} or empty)
         * @param delim the delimiter to use (typically a ",")
         * @return the delimited {@code String}
         */
        public static String collectionToDelimitedString(@Nullable Collection<?> coll, String delim) {
            return collectionToDelimitedString(coll, delim, "", "");
        }

        /**
         * Convert a {@code Collection} into a delimited {@code String} (e.g., CSV).
         * <p>Useful for {@code toString()} implementations.
         * @param coll the {@code Collection} to convert (potentially {@code null} or empty)
         * @return the delimited {@code String}
         */
        public static String collectionToCommaDelimitedString(@Nullable Collection<?> coll) {
            return collectionToDelimitedString(coll, ",");
        }

        /**
         * Convert a {@code String} array into a delimited {@code String} (e.g. CSV).
         * <p>Useful for {@code toString()} implementations.
         * @param arr the array to display (potentially {@code null} or empty)
         * @param delim the delimiter to use (typically a ",")
         * @return the delimited {@code String}
         */
        public static String arrayToDelimitedString(@Nullable Object[] arr, String delim) {
            if (ObjectUtils.isEmpty(arr)) {
                return "";
            }
            if (arr.length == 1) {
                return ObjectUtils.nullSafeToString(arr[0]);
            }

            StringJoiner sj = new StringJoiner(delim);
            for (Object elem : arr) {
                sj.add(String.valueOf(elem));
            }
            return sj.toString();
        }

        /**
         * Convert a {@code String} array into a comma delimited {@code String}
         * (i.e., CSV).
         * <p>Useful for {@code toString()} implementations.
         * @param arr the array to display (potentially {@code null} or empty)
         * @return the delimited {@code String}
         */
        public static String arrayToCommaDelimitedString(@Nullable Object[] arr) {
            return arrayToDelimitedString(arr, ",");
        }

    }
}
