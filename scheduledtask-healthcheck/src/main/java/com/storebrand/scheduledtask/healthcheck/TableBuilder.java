package com.storebrand.scheduledtask.healthcheck;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Helper class to create a nicely formatted text table.
 *
 * This class can be used to create a textual table. Add headings, then continuously add rows, and then call {@link
 * #toString()} to produce a table.
 *
 * @author Ståle Undheim, 2020.10.01 <staale.undheim@storebrand.no>
 * @author Dag Bertelsen, 2021.04 <dag.lennart.bertelsen@storebrand.no>
 */
class TableBuilder {

    private final String[] _headings;
    private final List<Object[]> _rows = new ArrayList<>();

    /**
     * Create a table builder with the given headings.
     *
     * This sets the column count for the table, and all addRow calls must have exactly the same number of column
     * values.
     *
     * @param headings
     *         column headings for the table
     */
    TableBuilder(String... headings) {
        _headings = headings;
    }

    /**
     * Add a row to this table.
     *
     * The number of values must match exactly the number of headings. Null values are replaced by blank, {@link Number}
     * values will be right aligned, while all other values are left aligned using toString.
     *
     * @param values row values to add to the table
     * @return this builder, with the values added.
     */
    TableBuilder addRow(Object... values) {
        if (values.length != _headings.length) {
            throw new IllegalArgumentException("Expected [" + _headings.length + "] values, "
                    + "but got [" + values.length + "] values, can not add row");
        }
        _rows.add(values);
        return this;
    }

    /**
     * Convert the table data to a string.
     *
     * This will creat a table, where each column is set wide enough to accomodate all values in the table.
     * The table will not end with a newline.
     *
     * @return the rendered table for this {@link TableBuilder}.
     */
    @Override
    public String toString() {
        // Calculate the max width for each column, starting with the heading length
        int[] columnMaxWidth = Stream.of(_headings).mapToInt(String::length).toArray();

        // Go through each row, then each column, and check the toString length of each value, and increase that
        // column max width if necessary.
        for (Object[] row : _rows) {
            for (int i = 0; i < row.length; i++) {
                if (row[i] != null) {
                    columnMaxWidth[i] = Math.max(columnMaxWidth[i], row[i].toString().length());
                }
            }
        }

        // Each row is as wide as the sum of max widths, plus 3 characters separator between each column, and a newline
        int rowWidth = IntStream.of(columnMaxWidth).sum() + (columnMaxWidth.length - 1) * 3 + 1;

        // We have a total of _rows.size() rows, plus heading and separator. We get one extra newline at the last row
        // that we won't use.
        StringBuilder result = new StringBuilder(rowWidth * (_rows.size() + 2));

        // Print the header
        for (int i = 0; i < _headings.length; i++) {
            if (i != 0) {
                result.append(" | ");
            }
            String heading = _headings[i];
            result.append(heading);
            int padding = columnMaxWidth[i] - heading.length();
            for (int j = 0; j < padding; j++) {
                result.append(" ");
            }
        }
        result.append("\n");

        // Add a separator before the table
        for (int i = 0; i < columnMaxWidth.length; i++) {
            if (i != 0) {
                result.append("-+-");
            }
            for (int j = 0; j < columnMaxWidth[i]; j++) {
                result.append("-");
            }
        }

        // ?: Is the table empty
        if (_rows.isEmpty()) {
            // Yes -> Add a line informing that there is no data in the table
            String noData = "-- No rows in table --";
            result.append("\n");
            for (int i = 0; i < Math.max(0, rowWidth / 2 - noData.length()); i++) {
                result.append(" ");
            }
            result.append(noData);
        }

        // append each row
        for (Object[] row : _rows) {
            result.append("\n");
            for (int i = 0; i < row.length; i++) {
                Object columnValue = row[i];
                int columnWidth = columnMaxWidth[i];
                if (i != 0) {
                    result.append(" | ");
                }
                // ?: Is this a null value
                if (columnValue == null) {
                    // Yes -> print blank
                    for (int j = 0; j < columnWidth; j++) {
                        result.append(" ");
                    }
                }
                // ?: Is this a number
                else if (columnValue instanceof Number) {
                    // Yes -> then right align
                    String text = String.valueOf(columnValue);
                    for (int j = 0; j < columnWidth - text.length(); j++) {
                        result.append(" ");
                    }
                    result.append(text);
                }
                else {
                    // E -> Left align.
                    String text = String.valueOf(columnValue);
                    result.append(text);
                    for (int j = 0; j < columnWidth - text.length(); j++) {
                        result.append(" ");
                    }
                }
            }
        }
        return result.toString();
    }
}