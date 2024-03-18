package com.sql.poc;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.AfterAll;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SqlScriptBatchExecutorUnitTest {
    private static final Log logger = LogFactory.getLog(SqlScriptBatchExecutorUnitTest.class);
    private static Connection connection = null;
    private static final String JDBC_URL = "jdbc:h2:mem:testdb3";
    private static final String USERNAME = "user";
    private static final String PASSWORD = "password";

    @Before
    public void prepareConnection() throws Exception {
        connection = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);
    }

    @AfterAll
    public static void closeConnection() throws Exception {
        connection.close();
    }

    @Test
    public void givenConnectionObject_whenSQLFile_thenExecute() throws Exception {
        String path = new File(ClassLoader.getSystemClassLoader()
                .getResource("employee.sql").getFile()).toPath().toString();
//Creating a File object for directory
//        File directoryPath = new File("C:\\TestSQLFiles" );
//        //List of all files and directories
//        File filesList[] = directoryPath.listFiles();
//        System.out.println("List of files and directories in the specified directory:");
//        for(File file : filesList) {
//            System.out.println("File name: "+file.getName());
//            System.out.println("File path: "+file.getAbsolutePath());
//            System.out.println("Size :"+file.getTotalSpace());
//            System.out.println(" ");
//        }
        long start = System.currentTimeMillis();
        SqlScriptBatchExecutor.executeBatchedSQL(path, connection, 20);
        long end = System.currentTimeMillis();
        System.out.println("Elapsed Time in milli seconds: "+ (end-start));
        Statement statement = connection.createStatement();
        //the expression "1" evaluates to non-null for every row, and since you are not removing duplicates,
        // COUNT(1) should always return the same number as COUNT(*)
        ResultSet resultSet = statement.executeQuery("SELECT COUNT(1) FROM employees");

        if (resultSet.next()) {
            int count = resultSet.getInt(1);
            Assert.assertEquals("Incorrect number of records inserted", 20, count);
        }
    }

}
