package com.storebrand.scheduledtask.db.sql;

import java.net.URL;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Will check and see if a table exists in the database and retrieve the {@link TableColumn}s and Primary keys names so
 * the calling code can inspect to see if all the fields are in this database.
 *
 * @see <a href="https://www.progress.com/blogs/jdbc-tutorial-extracting-database-metadata-via-jdbc-driver">JDBC
 * Tutorial: Extracting Database Metadata via JDBC</>.
 */
class TableInspector {
    private static final Logger log = LoggerFactory.getLogger(TableInspector.class);
    public static final String TABLE_VERSION = "stb_schedule_table_version";
    public static final int VALID_VERSION = 1;
    private final Map<String, TableColumn> _tableColumns;
    private final Map<String, String> _primaryKeys;
    private final Map<String, ForeignKey> _foreignKeys;
    private final String _tableName;
    private static final String MIGRATION_FILE_NAME = "V_1__Create_initial_tables.sql";
    private final DataSource _dataSource;

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "False positive in JDK 11")
    public TableInspector(DataSource dataSource, String tableName) {
        _tableName = tableName;
        _dataSource = dataSource;
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            _tableColumns = extractColumns(metaData, tableName.toUpperCase());
            _primaryKeys = extractPrimaryKeys(metaData, tableName.toUpperCase());
            _foreignKeys = extractForeignKeys(metaData, tableName.toUpperCase());
        }
        catch (SQLException throwables) {
            throw new TableValidationException("Unable to retrieve the metadata for the given dataSource. "
                    + getMigrationLocationMessage(), throwables);
        }
    }

    /**
     * Checks the table version table to see if the database are using the correct {@link #MIGRATION_FILE_NAME} version.
     *
     */
    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "False positive in JDK 11")
    public int getTableVersion() {
        String sql = "SELECT * FROM " + TABLE_VERSION;

        try (Connection sqlConnection = _dataSource.getConnection();
             PreparedStatement pStmt = sqlConnection.prepareStatement(sql);
             ResultSet result = pStmt.executeQuery()) {

            if (result.next()) {
                return result.getInt("version");
            }
        }
        catch (SQLException throwables) {
            throw new RuntimeException(getMigrationLocationMessage(), throwables);
        }
        return 0;
    }

    /**
     * The amount of column found in the specified table.
     */
    public int amountOfColumns() {
        return _tableColumns.size();
    }

    /**
     * Checks if we found the column with the given name
     */
    public boolean columnExists(String columnName) {
        return _tableColumns.containsKey(columnName.toUpperCase());
    }

    /**
     * Helper method to quickly check if a column has the specified parameters without verifying the column size.
     * @param columnName - Name of the column to check
     * @param isNullable - Check if this column can be set to <b>null</b>
     * @param dataTypes - The {@link Types} this column should have. Note since there is multiple valid types like
     *                    {@link Types#VARCHAR} and {@link Types#NVARCHAR} we can supply a list of types it should
     *                    have one of. Due to different databases may use different types we may need to supply multiple
     *                    types that should be a valid option
     */
    public void validateColumn(String columnName, boolean isNullable, JDBCType... dataTypes) {
        validateColumn(columnName, null, isNullable, dataTypes);
    }

    /**
     * Helper method to quickly check if a column has the specified parameters.
     *
     * @param columnName - Name of the column to check
     * @param minColumnSize - The size of the column. Usually used with columns like {@link Types#VARCHAR}.
     *                        If set to null this check will be skipped.
     * @param isNullable - Check if this column can be set to <b>null</b>
     * @param dataTypes - The {@link Types} this column should have. Note since there is multiple valid types like
     *                    {@link Types#VARCHAR} and {@link Types#NVARCHAR} we can supply a list of types it should
     *                    have one of. Due to different databases may use different types we may need to supply multiple
     *                    types that should be a valid option
     */
    @SuppressWarnings({"BooleanExpressionComplexity"})
    public void validateColumn(String columnName, Integer minColumnSize, boolean isNullable, JDBCType... dataTypes) {
        // ?: Did we find the table column?
        if (!columnExists(columnName)) {
            // E-> No, column not found
            throw new TableValidationException("Table column '" + columnName + "' "
                    + "where not found in the table '" + _tableName + "' " + getMigrationLocationMessage());
        }
        // E-> Yes we did find the column
        List<JDBCType> validTypes = Arrays.asList(dataTypes);
        // :? Validate that the column has the specified parameters
        TableColumn columnToCheck = _tableColumns.get(columnName.toUpperCase());
        if (validTypes.contains(columnToCheck.getDataType())
                // if columnSize == null we should not validate this column:
                && (minColumnSize == null || columnToCheck.getColumnSize() >= minColumnSize)
                && columnToCheck.isNullable().orElse(false) == isNullable) {
            // -> Yes, this column has the correct parameters
            return;
        }

        // E-> No the table column has not the correct size
        log.info("Table '" + _tableName + "' column '" + columnName + "' where invalid, found: " + columnToCheck
                + ", expected to find one of dataTypes:'" + validTypes + "', minColumnSize:'" + minColumnSize + "'"
                + ", isNullable:'" + isNullable + "'");
        String errorMessage = "Table column '" + columnName + "' in table '" + _tableName + "'"
                + " should have one of the dataTypes '" + validTypes + "'"
                + " and should be set to " + (isNullable ? "'NULL'" : "'NOT NULL'.");
        errorMessage = errorMessage + "Column size should be at least '" + minColumnSize + "'";
        errorMessage = errorMessage + " " + getMigrationLocationMessage();
        throw new TableValidationException(errorMessage);
    }

    /**
     * Helper method to check if a table has a primary key with a specific name.
     */
    public boolean hasPrimaryKey(String columnName, String primaryKeyName) {
        // ?: Does this column have a private key at all
        if (!_primaryKeys.containsKey(columnName.toUpperCase())) {
            // -> No, this column does not have any primary keys
            return false;
        }

        // E-> Yes we have a primary key set but check if it has the same name:
        return _primaryKeys.get(columnName).equalsIgnoreCase(primaryKeyName);
    }

    /**
     * Check if a given column has a foreign key set.
     *
     * @param columnName - The user column that has this foreign key set. IE this column has a foreign key that is
     * another table columns primary key
     * @param keyOwnerTableName - The table name of the table that is the owner of the key. IE this has this set as a
     * primary key and 'OWNS' this field.
     * @param keyOwnerColumnName - The column name that is the owner of the key. IE this has a primary key and 'OWNS' the
     * field.
     */
    public boolean hasForeignKey(String columnName, String keyOwnerTableName, String keyOwnerColumnName) {
        // ?: Does this column have a private key at all
        if (!_foreignKeys.containsKey(columnName.toUpperCase())) {
            // -> No, this column does not have any primary keys
            return false;
        }

        // E-> Yes we have a primary key set but check if it has the same name:
        return _foreignKeys.get(columnName.toUpperCase()).getPrimaryKeyOwnerTable().equalsIgnoreCase(keyOwnerTableName)
                && _foreignKeys.get(columnName.toUpperCase()).getPrimaryKeyOwnerColumn().equalsIgnoreCase(keyOwnerColumnName);
    }

    /**
     * Returns the location of the migrationfile containing the creation of the required tables.
     */
    public final String getMigrationFileLocation() {
        URL codeSourceLocation = this.getClass().getProtectionDomain().getCodeSource().getLocation();
        return codeSourceLocation + "com/storebrand/scheduledtask/db/sql/" + MIGRATION_FILE_NAME;
    }

    public final String getMigrationLocationMessage() {
        return "Make sure you are using the migration script "
                + "'" + getMigrationFileLocation() + "' to create the required tables";
    }

    /**
     * Retrieve the map containing the information on the table columns.
     */
    public Map<String, TableColumn> getTableColumns() {
        return Collections.unmodifiableMap(_tableColumns);
    }

    /**
     * Retrieve the map containing the primary keys for columns
     */
    public Map<String, String> getPrimaryKeys() {
        return Collections.unmodifiableMap(_primaryKeys);
    }

    @SuppressFBWarnings(value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
            justification = "False positive in JDK 11")
    private Map<String, TableColumn> extractColumns(DatabaseMetaData metaData, String tableName) {
        // Get the table columns
        try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {

            Map<String, TableColumn> tableColumns = new HashMap<>();
            // Loop through all columns an extract the sugar
            while (columns.next()) {
                TableColumn tableColumn = new TableColumn(
                        columns.getString("COLUMN_NAME").toUpperCase(),
                        columns.getInt("DATA_TYPE"),
                        columns.getString("TYPE_NAME"),
                        columns.getInt("COLUMN_SIZE"),
                        columns.getString("IS_NULLABLE"),
                        columns.getString("IS_AUTOINCREMENT")
                );
                tableColumns.put(tableColumn.columnName, tableColumn);
            }

            return tableColumns;
        }
        catch (SQLException throwable) {
            throw new TableValidationException("Unable to retrieve the columns for the table [" + tableName + "]",
                    throwable);
        }
    }

    private Map<String, String> extractPrimaryKeys(DatabaseMetaData metaData, String tableName) {
        try (ResultSet keys = metaData.getPrimaryKeys(null, null, tableName.toUpperCase())) {

            Map<String, String> primaryKeys = new HashMap<>();
            while (keys.next()) {
                String columnName = keys.getString("COLUMN_NAME");
                String primaryKeyName = keys.getString("PK_NAME");
                primaryKeys.put(columnName, primaryKeyName);
            }
            return primaryKeys;
        }
        catch (SQLException throwable) {
            throw new TableValidationException("Unable to retrieve the primary keys for the table [" + tableName + "]",
                    throwable);
        }
    }

    private Map<String, ForeignKey> extractForeignKeys(DatabaseMetaData metaData, String tableName) {
        try (ResultSet keys = metaData.getImportedKeys(null, null, tableName.toUpperCase())) {

            Map<String, ForeignKey> foreignKeys = new HashMap<>();
            while (keys.next()) {
                String columnName = keys.getString("FKCOLUMN_NAME");
                ForeignKey foreignKey = new ForeignKey(
                        keys.getString("PKTABLE_NAME"),
                        keys.getString("PKCOLUMN_NAME"),
                        keys.getString("FKTABLE_NAME"),
                        keys.getString("FKCOLUMN_NAME")
                );
                foreignKeys.put(columnName, foreignKey);
            }
            return foreignKeys;
        }
        catch (SQLException throwable) {
            throw new TableValidationException("Unable to retrieve the foreign keys for the table [" + tableName + "]",
                    throwable);
        }
    }

    /**
     * Class that holds the column information
     */
    public static class TableColumn {
        private final String columnName;
        private final int dataType;
        private final String typeName;
        private final int columnSize;
        private final String nullable;
        private final String autoIncrement;

        public TableColumn(String columnName, int dataType, String typeName, int columnSize, String isNullable,
                String isAutoIncrement) {
            this.columnName = columnName;
            this.typeName = typeName;
            this.dataType = dataType;
            this.columnSize = columnSize;
            this.nullable = isNullable;
            this.autoIncrement = isAutoIncrement;
        }

        public String getColumnName() {
            return columnName;
        }

        /**
         * The SQL type from {@link Types}
         */
        public String getTypeName() {
            return typeName;
        }

        /**
         * Get the data source dependent type name of the column in the db it self.
         */
        public JDBCType getDataType() {
            return JDBCType.valueOf(dataType);
        }

        public int getColumnSize() {
            return columnSize;
        }

        public Optional<Boolean> isNullable() {
            // ?: Is isNullable set
            if (nullable == null || nullable.isEmpty()) {
                // -> No, it is unknown so return optional empty
                return Optional.empty();
            }

            // E-> yes we have either yes or no here
            if ("no".equalsIgnoreCase(nullable)) {
                // -> No, it is not a nullable field.
                return Optional.of(false);
            }

            // E-> Yes this field is nullable
            return Optional.of(true);
        }

        public Optional<Boolean> isAutoIncrement() {
            // ?: Is autoIncrement set
            if (autoIncrement == null || autoIncrement.isEmpty()) {
                // -> No, it is unknown so return optional empty
                return Optional.empty();
            }

            // E-> yes we have either yes or no here
            if ("no".equalsIgnoreCase(autoIncrement)) {
                // -> No, it is not a auto incremented field.
                return Optional.of(false);
            }

            // E-> Yes, this field is autoIncremental
            return Optional.of(true);
        }

        @Override
        public String toString() {
            return "{ columnName:" + columnName + ", "
                    + "dataType:" + dataType + ", "
                    + "typeName:" + typeName + ", "
                    + "columnSize:" + columnSize + ", "
                    + "isNullable:" + nullable + ", "
                    + "isAutoIncrement:" + autoIncrement + "}";
        }
    }

    /**
     * Exception thrown when the table we try to validate either can't be found or there is some differences on what we
     * expect and what it has been set up to use.
     */
    public static class TableValidationException extends RuntimeException {
        public TableValidationException(String message, Throwable cause) {
            super(message, cause);
        }

        public TableValidationException(String message) {
            super(message);
        }
    }

    /**
     * Holds the Primary key owner tableName and column. Note by Owner it means the table that actually has that column
     * as a primary key. It also holds the foreign key tableName and column, by this it means the tableName and
     * columnName of the table that actually uses the the field as a foreign key.
     *
     */
    private static class ForeignKey {
        private final String primaryKeyOwnerTable;
        private final String primaryKeyOwnerColumn;
        private final String foreignKeyTable;
        private final String foreignKeyColumn;

        ForeignKey(String primaryKeyOwnerTable, String primaryKeyOwnerColumn, String foreignKeyTable,
                String foreignKeyColumn) {
            this.primaryKeyOwnerTable = primaryKeyOwnerTable;
            this.primaryKeyOwnerColumn = primaryKeyOwnerColumn;
            this.foreignKeyTable = foreignKeyTable;
            this.foreignKeyColumn = foreignKeyColumn;
        }

        /**
         * Name of the table that has this as a primary key, IE the owner of the primary key.
         */
        public String getPrimaryKeyOwnerTable() {
            return primaryKeyOwnerTable;
        }

        /**
         * Name of the column that has this as a primary key, IE the owner of the primary key.
         */
        public String getPrimaryKeyOwnerColumn() {
            return primaryKeyOwnerColumn;
        }

        /**
         * Name of the table that uses this key. IE this table B uses the other table A's Primary key as ForeignKey.
         */
        public String getForeignKeyTable() {
            return foreignKeyTable;
        }

        /**
         * Name of the column that uses this key. IE this table B uses the other table A's Primary key as ForeignKey.
         */
        public String getForeignKeyColumn() {
            return foreignKeyColumn;
        }
    }
}

