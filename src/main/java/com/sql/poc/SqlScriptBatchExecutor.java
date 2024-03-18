package com.sql.poc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlScriptBatchExecutor {
    private static final Log logger = LogFactory.getLog(SqlScriptBatchExecutor.class);
    private static final Pattern COMMENT_PATTERN = Pattern.compile("--.*|/\\*(.|[\\r\\n])*?\\*/");
    public static void executeBatchedSQL(String scriptFilePath, Connection connection, int batchSize) throws Exception {
        List<String> sqlStatements = parseSQLScript(scriptFilePath);
        executeSQLBatches(connection, sqlStatements, batchSize);
    }
    private static List<String> parseSQLScript(String scriptFilePath) throws IOException {
        List<String> sqlStatements = new ArrayList<>();
        // Wrap the FileReader in a BufferedReader for efficient reading.
        try (BufferedReader reader = new BufferedReader(new FileReader(scriptFilePath))) {
            StringBuilder currentStatement = new StringBuilder();
            String line;
            int lineNumber = 0;
            // Read lines from the SQL file until the end of the
            // file is reached.
            while ((line = reader.readLine()) != null) {
                lineNumber += 1;
                Matcher commentMatcher = COMMENT_PATTERN.matcher(line);
                line = commentMatcher.replaceAll("");
                line = line.trim();
                // Skip empty lines and single-line comments.
                if (line.isEmpty() || line.startsWith("--")) {
                    continue;
                }

                currentStatement.append(line).append(" ");

                // If the line ends with a semicolon, it
                // indicates the end of an SQL command.
                if (line.endsWith(";")) {
                    sqlStatements.add(currentStatement.toString()); //add and run in once as batch.
                    logger.info(currentStatement.toString());
                    currentStatement.setLength(0);
                }
            }
        }  catch (IOException e) {
            throw e;
        }
        return sqlStatements;
    }

    private static void executeSQLBatches(Connection connection, List<String> sqlStatements, int batchSize)
            throws SQLException {
        int count = 0;
        // Create a statement object to execute SQL commands.
        Statement statement = connection.createStatement();

        for (String sql : sqlStatements) {
            statement.addBatch(sql);
            count++;

            if (count % batchSize == 0) {
                logger.info("Executing batch");
                statement.executeBatch();
                statement.clearBatch();
            }
        }
        // Execute any remaining statements
        if (count % batchSize != 0) {
            statement.executeBatch();
        }
        connection.commit();
    }
}
