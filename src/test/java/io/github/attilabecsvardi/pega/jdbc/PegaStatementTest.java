package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.condition.EnabledIf;

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
    @Disabled("already tested")
    void executeQuery() {
    }

    @Test
    @Disabled("already tested")
    void executeUpdate() {
    }

    @Test
    @Order(1)
    void testExecute_CreateTable() throws SQLException {
        String createString =
                "create table data.UNIT_TEST_EMP " +
                        "(ID integer NOT NULL PRIMARY KEY, " +
                        "NAME varchar(40) NOT NULL)";
        stmt.executeUpdate("drop table if exists data.UNIT_TEST_EMP");
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
        String dropString =
                "drop table data.UNIT_TEST_EMP";

        int ret = stmt.executeUpdate(dropString);
        assertEquals(0, ret);

        String query = "select ID, NAME, CITY " +
                "from data.UNIT_TEST_EMP ";
        assertThrows(SQLException.class, () -> {
            stmt.executeQuery(query);
        });
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
        Statement s = conn.createStatement();
        assertFalse(s.isClosed());
        s.close();
        assertTrue(s.isClosed());
        // method call on closed connection throws exception
        assertThrows(SQLException.class, () -> s.executeQuery("select 1"));
    }

    @Test
    public void testDoubleClose() throws SQLException {
        Statement stmtTemp = conn.createStatement();
        stmtTemp.close();
        stmtTemp.close();
    }

    @Test
    public void testMultiExecute() throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS test_statement");
        stmt.execute("CREATE TABLE test_statement(i int)");
        assertTrue(stmt.execute("SELECT 1 as a; UPDATE test_statement SET i=1; SELECT 2 as b, 3 as c"));

        ResultSet rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));
        rs.close();

        assertTrue(!stmt.getMoreResults());
        assertEquals(0, stmt.getUpdateCount());

        assertTrue(stmt.getMoreResults());
        rs = stmt.getResultSet();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));
        rs.close();

        assertTrue(!stmt.getMoreResults());
        assertEquals(-1, stmt.getUpdateCount());

        stmt.execute("DROP TABLE test_statement");
    }

    @Test
    public void testEmptyQuery() throws SQLException {
        stmt.execute("");
        assertNull(stmt.getResultSet());
        assertTrue(!stmt.getMoreResults());
    }

    @Test
    @EnabledIf("isPostgresDB")
    public void testNumericFunctions() throws SQLException {

        ResultSet rs = stmt.executeQuery("select {fn abs(-2.3)} as abs ");
        assertTrue(rs.next());
        assertEquals(2.3f, rs.getFloat(1), 0.00001);

        rs = stmt.executeQuery("select {fn acos(-0.6)} as acos ");
        assertTrue(rs.next());
        assertEquals(Math.acos(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn asin(-0.6)} as asin ");
        assertTrue(rs.next());
        assertEquals(Math.asin(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn atan(-0.6)} as atan ");
        assertTrue(rs.next());
        assertEquals(Math.atan(-0.6), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn atan2(-2.3,7)} as atan2 ");
        assertTrue(rs.next());
        assertEquals(Math.atan2(-2.3, 7), rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn ceiling(-2.3)} as ceiling ");
        assertTrue(rs.next());
        assertEquals(-2, rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn cos(-2.3)} as cos, {fn cot(-2.3)} as cot ");
        assertTrue(rs.next());
        assertEquals(Math.cos(-2.3), rs.getDouble(1), 0.00001);
        assertEquals(1 / Math.tan(-2.3), rs.getDouble(2), 0.00001);

        rs = stmt.executeQuery("select {fn degrees({fn pi()})} as degrees ");
        assertTrue(rs.next());
        assertEquals(180, rs.getDouble(1), 0.00001);

        rs = stmt.executeQuery("select {fn exp(-2.3)}, {fn floor(-2.3)},"
                + " {fn log(2.3)},{fn log10(2.3)},{fn mod(3,2)}");
        assertTrue(rs.next());
        assertEquals(Math.exp(-2.3), rs.getDouble(1), 0.00001);
        assertEquals(-3, rs.getDouble(2), 0.00001);
        assertEquals(Math.log(2.3), rs.getDouble(3), 0.00001);
        assertEquals(Math.log(2.3) / Math.log(10), rs.getDouble(4), 0.00001);
        assertEquals(1, rs.getDouble(5), 0.00001);

        rs = stmt.executeQuery("select {fn pi()}, {fn power(7,-2.3)},"
                + " {fn radians(-180)},{fn round(3.1294,2)}");
        assertTrue(rs.next());
        assertEquals(Math.PI, rs.getDouble(1), 0.00001);
        assertEquals(Math.pow(7, -2.3), rs.getDouble(2), 0.00001);
        assertEquals(-Math.PI, rs.getDouble(3), 0.00001);
        assertEquals(3.13, rs.getDouble(4), 0.00001);

        rs = stmt.executeQuery("select {fn sign(-2.3)}, {fn sin(-2.3)},"
                + " {fn sqrt(2.3)},{fn tan(-2.3)},{fn truncate(3.1294,2)}");
        assertTrue(rs.next());
        assertEquals(-1, rs.getInt(1));
        assertEquals(Math.sin(-2.3), rs.getDouble(2), 0.00001);
        assertEquals(Math.sqrt(2.3), rs.getDouble(3), 0.00001);
        assertEquals(Math.tan(-2.3), rs.getDouble(4), 0.00001);
        assertEquals(3.12, rs.getDouble(5), 0.00001);
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

    // used for postgres specific tests
    boolean isPostgresDB() {
        return config.getProperty("dbType").equalsIgnoreCase("postgres");
    }
}