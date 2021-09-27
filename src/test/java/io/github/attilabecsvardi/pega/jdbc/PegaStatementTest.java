package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class PegaStatementTest {

    static Properties config;
    static String url;
    static Connection conn;
    static Statement stmt;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        config = new Properties();
        try (InputStream is = new FileInputStream("src/test/resources/unit_test.properties")) {
            config.load(is);
            url = config.getProperty("url");
        } catch (Exception e) {
            throw e;
        }

        // instantiate a new connection object
        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        conn = DriverManager.getConnection(url, config);

        if (conn == null) {
            throw new Exception("Failed to create new Connection");
        }

        stmt = conn.createStatement();

        if (stmt == null) {
            throw new Exception("Failed to create new Statement");
        }
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    @Disabled
    void executeQuery() {
    }

    @Test
    @Disabled
    void executeUpdate() {
    }

    @Test
    @Order(1)
    void testExecute_CreateTable() throws SQLException {
        String createString =
                "create table data.UNIT_TEST_EMP " +
                        "(ID integer NOT NULL PRIMARY KEY, " +
                        "NAME varchar(40) NOT NULL)";

        int ret = stmt.executeUpdate(createString);
        assertEquals(0, ret);
    }

    @Test
    @Order(2)
    void testExecute_AlterTable() throws SQLException {
        String alterString =
                "alter table data.UNIT_TEST_EMP " +
                        "add column CITY varchar(40)";

        int ret = stmt.executeUpdate(alterString);
        assertEquals(0, ret);
    }

    @Test
    @Order(3)
    void testExecute_InsertRows() throws SQLException {
        int ret = stmt.executeUpdate("insert into data.UNIT_TEST_EMP(ID, NAME, CITY) " +
                "values(1, 'TestName1', 'TestCity1')");
        assertEquals(1, ret);
        stmt.executeUpdate("insert into data.UNIT_TEST_EMP(ID, NAME, CITY) " +
                "values(2, 'TestName2', 'TestCity2')");
        assertEquals(1, ret);
    }

    @Test
    @Order(4)
    void testExecute_QueryRows() throws SQLException {
        String query = "select ID, NAME, CITY from data.UNIT_TEST_EMP order by ID";
        ResultSet rs = stmt.executeQuery(query);
        int count = 0;
        while (rs.next()) {
            count++;
            int id = rs.getInt(1);
            assertEquals(count, id);
            String name = rs.getString(2);
            assertEquals("TestName" + count, name);
            String city = rs.getString(3);
            assertEquals("TestCity" + count, city);
        }

        assertEquals(2, count);

        rs.close();
    }

    @Test
    @Order(5)
    void testExecute_UpdateRows() throws SQLException {
        String updateString =
                "update data.UNIT_TEST_EMP " +
                        "set CITY = 'city1' " +
                        "where ID = 1";

        int ret = stmt.executeUpdate(updateString);
        assertEquals(1, ret);

        String query = "select ID, NAME, CITY " +
                "from data.UNIT_TEST_EMP " +
                "where ID = 1";
        ResultSet rs = stmt.executeQuery(query);
        int count = 0;
        if (rs.next()) {
            count++;
            String city = rs.getString(3);
            assertEquals("city1", city);
        }

        assertEquals(1, count);
        rs.close();
    }

    @Test
    @Order(6)
    void testExecute_DeleteRows() throws SQLException {
        String deleteString =
                "delete from data.UNIT_TEST_EMP " +
                        "where ID = 1";

        int ret = stmt.executeUpdate(deleteString);
        assertEquals(1, ret);

        String query = "select ID, NAME, CITY " +
                "from data.UNIT_TEST_EMP " +
                "where ID = 1";
        ResultSet rs = stmt.executeQuery(query);
        int count = 0;
        if (rs.next()) {
            count++;
        }

        assertEquals(0, count);
        rs.close();
    }

    @Test
    @Order(7)
    void testExecute_TruncateTable() throws SQLException {
        String truncateString =
                "truncate table data.UNIT_TEST_EMP";

        int ret = stmt.executeUpdate(truncateString);
        assertEquals(0, ret);

        String query = "select ID, NAME, CITY " +
                "from data.UNIT_TEST_EMP ";
        ResultSet rs = stmt.executeQuery(query);
        int count = 0;
        if (rs.next()) {
            count++;
        }

        assertEquals(0, count);
        rs.close();
    }

    @Test
    @Order(8)
    void testExecute_DropTable() throws SQLException {
        String truncateString =
                "drop table data.UNIT_TEST_EMP";

        int ret = stmt.executeUpdate(truncateString);
        assertEquals(0, ret);

        String query = "select ID, NAME, CITY " +
                "from data.UNIT_TEST_EMP ";
        assertThrows(SQLException.class, () -> {
            stmt.executeQuery(query);
        });
    }

    @Test
    @Disabled
    void close() {
    }

    @Test
    @Disabled
    void isClosed() {
    }

    @Test
    void testClose() throws SQLException {
        Statement s = conn.createStatement();
        assertFalse(s.isClosed());
        s.close();
        assertTrue(s.isClosed());
        // method call on closed connection throws exception
        assertThrows(SQLException.class, () -> s.executeQuery("select 1"));
    }

    @Test
    void getMaxFieldSize() {
    }

    @Test
    void setMaxFieldSize() {
    }

    @Test
    void getMaxRows() {
    }

    @Test
    void setMaxRows() {
    }

    @Test
    void setEscapeProcessing() {
    }

    @Test
    void getQueryTimeout() {
    }

    @Test
    void setQueryTimeout() {
    }

    @Test
    void cancel() {
    }

    @Test
    void getWarnings() {
    }

    @Test
    void clearWarnings() {
    }

    @Test
    void setCursorName() {
    }

    @Test
    void execute() {
    }

    @Test
    void getResultSet() {
    }

    @Test
    void getUpdateCount() {
    }

    @Test
    void getMoreResults() {
    }

    @Test
    void getFetchDirection() {
    }

    @Test
    void setFetchDirection() {
    }

    @Test
    void getFetchSize() {
    }

    @Test
    void setFetchSize() {
    }

    @Test
    void getResultSetConcurrency() {
    }

    @Test
    void getResultSetType() {
    }

    @Test
    void addBatch() {
    }

    @Test
    void clearBatch() {
    }

    @Test
    void executeBatch() {
    }

    @Test
    void getConnection() {
    }

    @Test
    void testGetMoreResults() {
    }

    @Test
    void getGeneratedKeys() {
    }

    @Test
    void testExecuteUpdate() {
    }

    @Test
    void testExecuteUpdate1() {
    }

    @Test
    void testExecuteUpdate2() {
    }

    @Test
    void testExecute() {
    }

    @Test
    void testExecute1() {
    }

    @Test
    void testExecute2() {
    }

    @Test
    void getResultSetHoldability() {
    }


    @Test
    void isPoolable() {
    }

    @Test
    void setPoolable() {
    }

    @Test
    void closeOnCompletion() {
    }

    @Test
    void isCloseOnCompletion() {
    }

    @Test
    void unwrap() {
    }

    @Test
    void isWrapperFor() {
    }
}