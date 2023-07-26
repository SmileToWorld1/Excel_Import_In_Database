package msf.jdbc_tests;
import msf.utilities.ConfigurationReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.*;

public class WorkstationProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkstationProcessor.class);

    public static void main(String[] args) {
        try {
            String url = ConfigurationReader.get("db.url.rodex");
            String username = ConfigurationReader.get("db.username.rodex");
            String password = ConfigurationReader.get("db.password.rodex");
            int plantId = 68;

            try (Connection connection = DriverManager.getConnection(url, username, password)) {
                String selectQuery = "SELECT DISTINCT operation_no, workstation_no_first, workstation_no_second, workstation_no_third FROM dev.tbl_product_tree";
                try (PreparedStatement stmt = connection.prepareStatement(selectQuery)) {
                    try (ResultSet resultSet = stmt.executeQuery()) {
                        while (resultSet.next()) {
                            String operationNo = resultSet.getString("operation_no");
                            String workstationNo_first = resultSet.getString("workstation_no_first");
                            String workstationNo_second = resultSet.getString("workstation_no_second");
                            String workstationNo_third = resultSet.getString("workstation_no_third");

                            // Process the first workstation
                            if (workstationNo_first != null && !workstationNo_first.isEmpty()) {
                                String[] workstations = workstationNo_first.split(",");
                                for (String workstation : workstations) {
                                    // Insert or update the workstation
                                    int workStationId = getWorkStationId(connection, workstation.trim(), plantId);
                                    int operationId = getOperationId(connection,operationNo,plantId);
                                    if (workStationId != -1) {
                                        insertOrUpdateOperationWorkstation(connection, operationId, workStationId, true,"VERY_HIGH");
                                    }
                                }
                            }

                            // Process the second workstation
                            if (workstationNo_second != null && !workstationNo_second.isEmpty()) {
                                String[] workstations = workstationNo_second.split(",");
                                for (String workstation : workstations) {
                                    // Insert or update the workstation
                                    int workStationId = getWorkStationId(connection, workstation.trim(), plantId);
                                    int operationId = getOperationId(connection,operationNo,plantId);
                                    if (workStationId != -1) {
                                        insertOrUpdateOperationWorkstation(connection, operationId, workStationId, false,"HIGH");
                                    }
                                }
                            }

                            // Process the third workstation
                            if (workstationNo_third != null && !workstationNo_third.isEmpty()) {
                                String[] workstations = workstationNo_third.split(",");
                                for (String workstation : workstations) {
                                    // Insert or update the workstation
                                    int workStationId = getWorkStationId(connection, workstation.trim(), plantId);
                                    int operationId = getOperationId(connection,operationNo,plantId);
                                    if (workStationId != -1) {
                                        insertOrUpdateOperationWorkstation(connection, operationId, workStationId, false,"MEDIUM");
                                    }
                                }
                            }

                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static int getWorkStationId(Connection connection, String workstationNo, int plantId) throws SQLException {
        int workStationId = -1;
        String selectQuery = "SELECT DISTINCT workstation_id FROM das_new.tbl_workstations WHERE TRIM(workstation_no) = ? " +
                "AND plant_id = ? AND WORKSTATION_STATUS<>'DELETED' LIMIT 1";

        // Sorgu başlatmadan önce loglama
        logger.info("Getting workstation ID for workstation number: {}, plant ID: {}", workstationNo, plantId);

        try (PreparedStatement stmt = connection.prepareStatement(selectQuery)) {
            stmt.setString(1, workstationNo);
            stmt.setInt(2, plantId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    workStationId = resultSet.getInt("workstation_id");

                    // Başarıyla elde edilen workStationId'yi loglama
                    logger.info("Workstation ID found: {}", workStationId);
                } else {
                    // Hiç sonuç bulunamadığında da loglama
                    logger.warn("Workstation ID not found for workstation number: {}, plant ID: {}", workstationNo, plantId);
                }
            }
        } catch (SQLException e) {
            // Hata durumunda hata seviyesinde loglama
            logger.error("An SQL exception occurred while getting workstation ID.", e);
            throw e; // Hata durumunu uygun şekilde yönetebilirsiniz
        }
        return workStationId;
    }

    private static int getOperationId(Connection connection, String operationNo, int plantId) throws SQLException {
        int operationId = -1;
        String selectQuery = "SELECT DISTINCT operation_id FROM das_new.tbl_operations WHERE TRIM(operation_no) = ? AND plant_id = ? AND OPERATION_STATUS<>'DELETED' LIMIT 1";

        // Sorgu başlatmadan önce loglama
        logger.info("Getting operation ID for operation number: {}, plant ID: {}", operationNo, plantId);

        try (PreparedStatement stmt = connection.prepareStatement(selectQuery)) {
            stmt.setString(1, operationNo);
            stmt.setInt(2, plantId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                if (resultSet.next()) {
                    operationId = resultSet.getInt("operation_id");

                    // Başarıyla elde edilen operationId'yi loglama
                    logger.info("Operation ID found: {}", operationId);
                } else {
                    // Operasyon bulunamadığında da loglama ve yeni operasyon eklemeye başlamadan önce bilgi loglama
                    logger.info("Operation not found. Adding a new operation.");

                    // Yeni operasyon eklemeye başladığımızı loglama
                    logger.debug("Inserting a new operation: operationNo={}, plantId={}", operationNo, plantId);

                    String insertQuery = "INSERT INTO das_new.tbl_operations (operation_no, operation_name, plant_id, OPERATION_STATUS, outsource, transfer) " +
                            "VALUES (?, ?, ?, 'ACTIVE', 0, 0)";
                    try (PreparedStatement insertStmt = connection.prepareStatement(insertQuery, Statement.RETURN_GENERATED_KEYS)) {
                        insertStmt.setString(1, operationNo);
                        insertStmt.setString(2, operationNo);
                        insertStmt.setInt(3, plantId);
                        insertStmt.executeUpdate();

                        // Yeni operasyonun kimliğini alalım ve loglama
                        try (ResultSet generatedKeys = insertStmt.getGeneratedKeys()) {
                            if (generatedKeys.next()) {
                                operationId = generatedKeys.getInt(1);
                                logger.info("New operation ID generated: {}", operationId);
                            }
                        }
                    }
                }
            }
        } catch (SQLException e) {
            // Hata durumunda hata seviyesinde loglama
            logger.error("An SQL exception occurred while getting operation ID.", e);
            throw e; // Hata durumunu uygun şekilde yönetebilirsiniz
        }
        return operationId;
    }


    private static void insertOrUpdateOperationWorkstation(Connection connection, int operationId, int workStationId, boolean isDefault, String priority) throws SQLException {
        String selectQuery = "SELECT COUNT(*) FROM das_new.tbl_operation_workstations WHERE OPERATION_ID = ? AND WORKSTATION_ID = ?";
        try (PreparedStatement stmt = connection.prepareStatement(selectQuery)) {
            stmt.setInt(1, operationId);
            stmt.setInt(2, workStationId);
            try (ResultSet resultSet = stmt.executeQuery()) {
                int count = 0;
                if (resultSet.next()) {
                    count = resultSet.getInt(1);
                }

                // Loglama için kullanılacak mesaj ve parametreler
                String logMessage = "Operation workstation with operation ID: {}, workstation ID: {}, isDefault: {}, priority: {}";

                if (count == 0) {
                    String insertQuery = "INSERT INTO das_new.tbl_operation_workstations (OPERATION_ID, WORKSTATION_ID, default_operation, active, create_date, priority) " +
                            "VALUES (?, ?, ?, ?, ?, ?)";
                    try (PreparedStatement insertStmt = connection.prepareStatement(insertQuery)) {
                        insertStmt.setInt(1, operationId);
                        insertStmt.setInt(2, workStationId);
                        insertStmt.setInt(3, isDefault ? 1 : 0);
                        insertStmt.setInt(4, 1);
                        insertStmt.setDate(5, new Date(System.currentTimeMillis()));
                        insertStmt.setString(6, priority);
                        insertStmt.executeUpdate();

                        // Yeni operasyon istasyonu eklendiğinde loglama
                        logger.info("New operation workstation added. " + logMessage, operationId, workStationId, isDefault, priority);
                    }
                } else {
                    String updateQuery = "UPDATE das_new.tbl_operation_workstations SET priority = ?, default_operation = ? WHERE OPERATION_ID = ? AND WORKSTATION_ID = ?";
                    try (PreparedStatement updateStmt = connection.prepareStatement(updateQuery)) {
                        updateStmt.setString(1, priority);
                        updateStmt.setInt(2, isDefault ? 1 : 0);
                        updateStmt.setInt(3, operationId);
                        updateStmt.setInt(4, workStationId);
                        updateStmt.executeUpdate();

                        // Mevcut operasyon istasyonu güncellendiğinde loglama
                        logger.info("Existing operation workstation updated. " + logMessage, operationId, workStationId, isDefault, priority);
                    }
                }
            }
        } catch (SQLException e) {
            // Hata durumunda hata seviyesinde loglama
            logger.error("An SQL exception occurred while inserting or updating operation workstation. Operation ID: {}, Workstation ID: {}", operationId, workStationId, e);
            throw e; // Hata durumunu uygun şekilde yönetebilirsiniz
        }
    }
}

