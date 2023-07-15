package msf.jdbc_tests;

import msf.utilities.ConfigurationReader;
import msf.utilities.DBUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.junit.jupiter.api.Test;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static msf.utilities.DBUtils.*;

public class ExcelToDatabaseImporter {
    public static void main (String[] args) throws SQLException, IOException {
        // get Excel file path and db table name
        String excelFilePath = ConfigurationReader.get("excelFilePath");
        String dbTableName = ConfigurationReader.get("dbTableName");


        // Create a connection with ready credentials
        Connection connection = DBUtils.createConnection();

        Workbook workbook = new XSSFWorkbook(new FileInputStream(excelFilePath));
        Sheet sheet = workbook.getSheetAt(0);
        Row headerRow = sheet.getRow(0);

        // get column numbers for adding column names in String[]
        int numberOfColumns = headerRow.getLastCellNum();

        // create string array for getting column names
        String[] columnNames = new String[numberOfColumns];

        // get column names from excel data and save String []
        for (int i = 0; i < numberOfColumns; i++) {
            Cell headerCell = headerRow.getCell(i);
            columnNames[i] = getCellValueAsString(headerCell);
        }

        createTableIfNotExists(connection, dbTableName, columnNames);

        String insertQuery = getInsertQuery(dbTableName, columnNames);

        PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

        for (int rowIndex = 1; rowIndex <= sheet.getLastRowNum(); rowIndex++) {
            Row row = sheet.getRow(rowIndex);
            for (int columnIndex = 0; columnIndex < numberOfColumns; columnIndex++) {
                Cell cell = row.getCell(columnIndex);
                String value = getCellValueAsString(cell);
                preparedStatement.setString(columnIndex + 1, value);
            }
            preparedStatement.addBatch();
        }

        preparedStatement.executeBatch();
        System.out.println("Excel data imported to database successfully!");

        // If you don't want to null values in db, run this method
        setEmptyCellValuesToNull(connection, dbTableName);

        // please set column names what you want to update
        List<String> columnNamesToUpdate = new ArrayList<>();
        columnNamesToUpdate.add("tool_capacity");
        columnNamesToUpdate.add("operation_needed_person");
        columnNamesToUpdate.add("workstation_capacity_min");
        columnNamesToUpdate.add("workstation_capacity_max");
        columnNamesToUpdate.add("equipment_quantity");

        // If you want to remove '.0' chars from cells, run this method
        updateColumnValues(connection, dbTableName, columnNamesToUpdate);


    }


}

