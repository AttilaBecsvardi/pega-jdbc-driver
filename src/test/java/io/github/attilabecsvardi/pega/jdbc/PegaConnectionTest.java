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
    @Disabled("don't know a generic test case")
    void nativeSQL() {
    }

    @Test
    @Disabled("already tested")
    void getAutoCommit() {
    }

    @Test
    @Disabled("already tested")
    void setAutoCommit() {
    }

    @Test
    @Disabled("already tested")
    void commit() {
        fail("TOBE implemented");
    }

    @Test
    @Disabled("already tested")
    void rollback() {
        fail("TOBE implemented");
    }

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
    public void testTransactions() throws Exception {
        Statement stmt;
        ResultSet rs;

        testAutoCommitGetterSetter();

        // Now test commit
        int ret;
        stmt = conn.createStatement();
        stmt.executeUpdate("drop table if exists test_a");

        stmt.executeUpdate("create table test_a(imagename name, image oid, id int4)");
        ret = stmt.executeUpdate("insert into test_a (imagename,image,id) values ('comttest',1234,5678)");
        assertEquals(1, ret);
        conn.setAutoCommit(false);

        // Now update image to 9876 and commit
        stmt.executeUpdate("update test_a set image=9876 where id=5678");
        conn.commit();
        rs = stmt.executeQuery("select image from test_a where id=5678");
        assertTrue(rs.next());
        assertEquals(9876, rs.getInt(1));
        rs.close();

        // Now try to change it but rollback
        stmt.executeUpdate("update test_a set image=1111 where id=5678");
        conn.rollback();
        rs = stmt.executeQuery("select image from test_a where id=5678");
        assertTrue(rs.next());
        assertEquals(9876, rs.getInt(1)); // Should not change!
        rs.close();

        stmt.execute("drop table test_a");
        stmt.close();
    }


    @Test
    @Disabled("already tested")
    void close() {
    }

    @Test
    @Disabled("already tested")
    void isClosed() {
    }

    @Test
    void testClose() throws SQLException {
        Connection c = DriverManager.getConnection(url, config);
        assertFalse(c.isClosed());
        c.close();
        // in our case close() will also terminate the session on Pega server-side
        // is there any better solution?!
        // assertTrue(c.isClosed());
        assertThrows(SQLException.class, c::isClosed);

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
}