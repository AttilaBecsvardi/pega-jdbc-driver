package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.CookieHandler;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PegaConnectionTest {
    static Properties config;
    static String url;
    static Connection conn;

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
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testCreateStatement_Simple() throws SQLException {
        Statement stmt = conn.createStatement();
        assertNotNull(stmt);
        assertTrue(stmt instanceof PegaStatement);
        stmt.close();
    }

    @Test
    void testPrepareStatement_Simple() throws SQLException {
        PreparedStatement pStmt = conn.prepareStatement("");
        assertNotNull(pStmt);
        assertTrue(pStmt instanceof PegaPreparedStatement);
        pStmt.close();
    }

    @Test
    void testPrepareCall_Simple() throws SQLException {
        CallableStatement cStmt = conn.prepareCall("");
        assertNotNull(cStmt);
        assertTrue(cStmt instanceof PegaCallableStatement);
        cStmt.close();
    }

    @Test
    @Disabled
    void nativeSQL() {
    }

    @Test
    @Disabled
    void getAutoCommit() {
    }

    @Test
    @Disabled
    void setAutoCommit() {
    }

    @Test
    void testAutoCommitGetterSetter() throws SQLException {
        boolean orig = conn.getAutoCommit();
        conn.setAutoCommit(!orig);
        boolean tmp = conn.getAutoCommit();
        assertEquals(!orig, tmp);
        // restore
        conn.setAutoCommit(orig);
        tmp = conn.getAutoCommit();
        assertEquals(orig, tmp);
    }

    @Test
    void commit() {
        fail("TOBE implemented");
    }

    @Test
    void rollback() {
        fail("TOBE implemented");
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
        Connection c = getConnection();
        assertFalse(c.isClosed());
        c.close();
        assertTrue(c.isClosed());
        // method call on closed connection throws exception
        assertThrows(SQLException.class, c::getAutoCommit);
    }

    @Test
    void getMetaData() {
    }

    @Test
    void isReadOnly() {
    }

    @Test
    void setReadOnly() {
    }

    @Test
    void getCatalog() {
    }

    @Test
    void setCatalog() {
    }

    @Test
    void getTransactionIsolation() {
    }

    @Test
    void setTransactionIsolation() {
    }

    @Test
    void getWarnings() {
    }

    @Test
    void clearWarnings() {
    }

    @Test
    void testCreateStatement() {
    }

    @Test
    void testPrepareStatement() {
    }

    @Test
    void testPrepareCall() {
    }

    @Test
    void getTypeMap() {
    }

    @Test
    void setTypeMap() {
    }

    @Test
    void getHoldability() {
    }

    @Test
    void setHoldability() {
    }

    @Test
    void setSavepoint() {
    }

    @Test
    void testSetSavepoint() {
    }

    @Test
    void testRollback() {
    }

    @Test
    void releaseSavepoint() {
    }

    @Test
    void testCreateStatement1() {
    }

    @Test
    void testPrepareStatement1() {
    }

    @Test
    void testPrepareCall1() {
    }

    @Test
    void testPrepareStatement2() {
    }

    @Test
    void testPrepareStatement3() {
    }

    @Test
    void testPrepareStatement4() {
    }

    @Test
    void createClob() {
    }

    @Test
    void createBlob() {
    }

    @Test
    void createNClob() {
    }

    @Test
    void createSQLXML() {
    }

    @Test
    void isValid() {
    }

    @Test
    void setClientInfo() {
    }

    @Test
    void getClientInfo() {
    }

    @Test
    void testGetClientInfo() {
    }

    @Test
    void testSetClientInfo() {
    }

    @Test
    void createArrayOf() {
    }

    @Test
    void createStruct() {
    }

    @Test
    void getSchema() {
    }

    @Test
    void setSchema() {
    }

    @Test
    void abort() {
    }

    @Test
    void setNetworkTimeout() {
    }

    @Test
    void getNetworkTimeout() {
    }

    @Test
    void unwrap() {
    }

    @Test
    void isWrapperFor() {
    }

    // helper method to get a connection
    Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, config);
    }
}