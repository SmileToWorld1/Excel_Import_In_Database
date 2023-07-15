package msf.utilities;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DBUtils {
    private static Connection connection;
    private static Statement statement;
    private static ResultSet resultSet;
    private static int updatedQueryCount;

    public static Connection createConnection(String dbUrl,String dbUsername,String dbPassword) {
        try {
            connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }

    public static Connection createConnection() {
        String dbUrl = ConfigurationReader.get("db.url.local");
        String dbUsername = ConfigurationReader.get("db.username.local");
        String dbPassword = ConfigurationReader.get("db.password.local");
        try {
            connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return connection;
    }

    public static void executeQuery(String query) {
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            resultSet = statement.executeQuery(query);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public  static void updateQuery(String query){
        try {
            statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            updatedQueryCount = statement.executeUpdate(query);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static int getRowCount() throws Exception {
        resultSet.last();
        int rowCount = resultSet.getRow();
        resultSet.beforeFirst();
        return rowCount;
    }

    public static int getColumnCount() throws SQLException {
        int columnCount = 0;
        ResultSetMetaData rsmd = resultSet.getMetaData();
        columnCount = rsmd.getColumnCount();
        return columnCount;
    }

    public static void destroy() {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param query
     * @return List of columns returned in result set
     */
    public static List<String> getColumnNames(String query) {
        executeQuery(query);
        List<String> columnsName = new ArrayList<>();

        try {
            ResultSetMetaData  rsmd = resultSet.getMetaData();
            for (int i = 1; i <= getColumnCount(); i++) {
                columnsName.add(rsmd.getColumnName(i));
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return columnsName;
    }

    /**
     *
     * @param columnName
     * @return list of values of a single column from the result set
     */
    public static List<Object> getOneColumnDatas(String columnName) {
        List<Object> rowList = new ArrayList<>();
        try {
            resultSet.beforeFirst();
            while (resultSet.next()) {
                rowList.add(resultSet.getObject(columnName));
            }
            resultSet.beforeFirst();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return rowList;
    }

    /**
     *
     * @param columnIndex
     * @return list of values of a single column from the result set
     */
    public static List<Object> getOneColumnDatas(int columnIndex) {
        List<Object> rowList = new ArrayList<>();
        try {
            resultSet.beforeFirst();
            while (resultSet.next()) {
                rowList.add(resultSet.getObject(columnIndex));
            }
            resultSet.beforeFirst();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return rowList;
    }

    public static List<String> getExactRowDatasAsList(int rowNumber){
        List<String> getRowDataList = new ArrayList<>();
        try {
            resultSet.absolute(rowNumber);
            for (int i = 1; i <= getColumnCount(); i++) {
                String cellData = resultSet.getString(i);
                getRowDataList.add(cellData);
            }
            resultSet.beforeFirst();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return getRowDataList;
    }

    public static String getExactCellValue(int rowNumber, int columnIndex){
        String result = "";
        try {
            resultSet.absolute(rowNumber);
            result = resultSet.getString(columnIndex);
            resultSet.beforeFirst();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static String getExactCellValue(int rowNumber,String columnName){
        String result = "";
        try {
            resultSet.absolute(rowNumber);
            result = resultSet.getString(columnName);
            resultSet.beforeFirst();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     *
     * @param query
     * @return returns query result in a list of lists where outer list represents
     *         collection of rows and inner lists represent a single row
     */
    public static List<List<Object>> getQueryResultList(String query) {
        executeQuery(query);
        List<List<Object>> rowList = new ArrayList<>();
        ResultSetMetaData rsmd;
        try {
            rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    row.add(resultSet.getObject(i));
                }
                rowList.add(row);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return rowList;
    }

    /**
     *
     * @param query
     * @return returns query result in a list of maps where the list represents
     *         collection of rows and a map represents represent a single row with
     *         key being the column name
     */
    public static List<Map<String, Object>> getQueryResultMap(String query) {
        executeQuery(query);
        List<Map<String, Object>> rowList = new ArrayList<>();
        ResultSetMetaData rsmd;
        try {
            rsmd = resultSet.getMetaData();
            while (resultSet.next()) {
                Map<String, Object> colNameValueMap = new HashMap<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    colNameValueMap.put(rsmd.getColumnName(i), resultSet.getObject(i));
                }
                rowList.add(colNameValueMap);
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return rowList;
    }

    /**
     *
     * @param query
     * @return returns a single cell value. If the results in multiple rows and/or
     *         columns of data, only first column of the first row will be returned.
     *         The rest of the data will be ignored
     */
    public static Object getCellValue(String query) {
        return getQueryResultList(query).get(0).get(0);
    }
    /**
     *
     * @param query
     * @return returns a list of Strings which represent a row of data. If the query
     *         results in multiple rows and/or columns of data, only first row will
     *         be returned. The rest of the data will be ignored
     */
    public static List<Object> getRowList(String query) {
        return getQueryResultList(query).get(0);
    }
    /**
     *
     * @param query
     * @return returns a map which represent a row of data where key is the column
     *         name. If the query results in multiple rows and/or columns of data,
     *         only first row will be returned. The rest of the data will be ignored
     */
    public static Map<String, Object> getRowMap(String query) {
        return getQueryResultMap(query).get(0);
    }

    public static void createTableIfNotExists(Connection connection, String tableName, String[] columnNames) throws SQLException {
        StringBuilder query = new StringBuilder("CREATE TABLE IF NOT EXISTS ");
        query.append(tableName).append(" (");

        for (int i = 0; i < columnNames.length; i++) {
            query.append(columnNames[i]).append(" VARCHAR(255)");
            if (i < columnNames.length - 1) {
                query.append(", ");
            }
        }

        query.append(")");

        try (PreparedStatement statement = connection.prepareStatement(query.toString())) {
            statement.execute();
            System.out.println("createTableIfNotExists method runs successfully!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String getInsertQuery(String tableName, String[] columnNames) {
        StringBuilder query = new StringBuilder("INSERT INTO ");
        query.append(tableName).append(" (");

        for (int i = 0; i < columnNames.length; i++) {
            query.append(columnNames[i]);
            if (i < columnNames.length - 1) {
                query.append(", ");
            }
        }

        query.append(") VALUES (?");
        for (int i = 1; i < columnNames.length; i++) {
            query.append(", ?");
        }

        query.append(")");
        return query.toString();
    }

    public static String getCellValueAsString(Cell cell) {
        String value = "";
        if (cell != null) {
            CellType cellType = cell.getCellType();
            if (cellType == CellType.STRING) {
                value = cell.getStringCellValue();
            } else if (cellType == CellType.NUMERIC) {
                if (DateUtil.isCellDateFormatted(cell)) {
                    value = cell.getLocalDateTimeCellValue().toString();
                } else {
                    value = Double.toString(cell.getNumericCellValue());
                }
            } else if (cellType == CellType.BOOLEAN) {
                value = Boolean.toString(cell.getBooleanCellValue());
            } else if (cellType == CellType.BLANK) {
                value = "";
            } else if (cellType == CellType.FORMULA) {
                value = cell.getCellFormula();
            }
        }
        return value;
    }

    public static void setEmptyCellValuesToNull(Connection connection, String tableName) throws SQLException {
        String updateQuery = "UPDATE " + tableName + " SET %s = NULL WHERE %s = ''";

        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, tableName, null);

        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String formattedQuery = String.format(updateQuery, columnName, columnName);

            try (PreparedStatement statement = connection.prepareStatement(formattedQuery)) {
                statement.executeUpdate();
            }
        }

        System.out.println("Empty cell values set to NULL.");
    }

    public static void setEmptyCellValuesToNullWithColumnNames(Connection connection, String tableName, List<String> columnNames) throws SQLException {
        String updateQuery = "UPDATE " + tableName + " SET %s = NULL WHERE %s = ''";

        for (String columnName : columnNames) {
            String formattedQuery = String.format(updateQuery, columnName, columnName);

            try (PreparedStatement statement = connection.prepareStatement(formattedQuery)) {
                statement.executeUpdate();
            }
        }

        System.out.println("Empty cell values set to NULL.");
    }

    public static void updateColumnValues(Connection connection, String tableName, List<String> columnNames) throws SQLException {
        String updateQuery = "UPDATE " + tableName + " SET ";

        for (String columnName : columnNames) {
            updateQuery += columnName + " = REPLACE(" + columnName + ", '.0', ''), ";
        }

        // Son virgülü kaldırma
        updateQuery = updateQuery.substring(0, updateQuery.lastIndexOf(","));

        try (PreparedStatement statement = connection.prepareStatement(updateQuery)) {
            statement.executeUpdate();
        }

        System.out.println("Column values updated successfully.");
    }



}
