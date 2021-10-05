package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PegaResultSetTest {

    static final String QUERYSTRING = "select ID, NAME, CITY, AGE from data.UNIT_TEST_EMP order by ID";
    static final String TRUNCATESTRING = "drop table data.UNIT_TEST_EMP";
    static final String CREATESTRING =
            "create table data.UNIT_TEST_EMP " +
                    "(ID integer NOT NULL PRIMARY KEY, " +
                    "NAME varchar(40) NOT NULL, " +
                    "CITY varchar(40), " +
                    "AGE integer)";
    static final String INSERTSTRING_1 = "insert into data.UNIT_TEST_EMP(ID, NAME, CITY, AGE) " +
            "values(1, 'TestName1', 'TestCity1', 10)";
    static final String INSERTSTRING_2 = "insert into data.UNIT_TEST_EMP(ID, NAME, CITY, AGE) " +
            "values(2, 'TestName2', 'TestCity2', 20)";
    static final String DROPSTRING = "drop table id exists data.UNIT_TEST_EMP";
    static Properties config;
    static String url;
    static Connection conn;
    static Statement stmt;
    static ResultSet rs;

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

        init();
    }

    static void init() throws Exception {
        int ret;

        try {
            ret = stmt.executeUpdate(TRUNCATESTRING);
        } catch (Exception e) {
        }

        ret = stmt.executeUpdate(CREATESTRING);

        ret = stmt.executeUpdate(INSERTSTRING_1);
        if (ret != 1) throw new Exception("Insert failed");
        ret = stmt.executeUpdate(INSERTSTRING_2);
        if (ret != 1) throw new Exception("Insert failed");

        rs = stmt.executeQuery(QUERYSTRING);
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
        cleanup();
        if (rs != null) {
            rs.close();
        }
        if (stmt != null) {
            stmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    static void cleanup() throws SQLException {
        try {
            int ret = stmt.executeUpdate(DROPSTRING);
        } catch (Exception e) {
        }
    }

    @Test
    void testReuseSimpleResult() throws SQLException {
        ResultSet rs = stmt.executeQuery(QUERYSTRING);
        while (rs.next()) {
            rs.getString(1);
        }
        rs.close();
        rs = stmt.executeQuery(QUERYSTRING);
        while (rs.next()) {
            rs.getString(1);
        }
        rs.close();
    }

    @Test
    void testParseSpecialValues() throws SQLException {
        for (int i = -10; i < 10; i++) {
            testParseSpecialValue("" + ((long) Integer.MIN_VALUE + i));
            testParseSpecialValue("" + ((long) Integer.MAX_VALUE + i));
            BigInteger bi = BigInteger.valueOf(i);
            testParseSpecialValue(bi.add(BigInteger.valueOf(Long.MIN_VALUE)).toString());
            testParseSpecialValue(bi.add(BigInteger.valueOf(Long.MAX_VALUE)).toString());
        }
    }

    private void testParseSpecialValue(String x) throws SQLException {
        Object expected;
        // try to parse x value as BigDecimal, Long or Integer
        expected = new BigDecimal(x);
        try {
            expected = Long.decode(x);
            expected = Integer.decode(x);
        } catch (Exception e) {
            // ignore
        }

        ResultSet rs = stmt.executeQuery("select " + x);
        rs.next();
        Object o = rs.getLong(1);
        assertEquals(expected.getClass().getName(), o.getClass().getName());
        assertTrue(expected.equals(o));
    }

    @Test
    void testSubstringDataType() throws SQLException {
        ResultSet rs = stmt.executeQuery("select substr('test', 1, 1)");
        rs.next();
        assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
    }

    @Test
    void testColumnLabelColumnName() throws SQLException {
        ResultSet rs = stmt.executeQuery("select 'test' as y");
        rs.next();
        rs.getString("x");
        rs.getString("y");
        rs.close();
        rs = conn.getMetaData().getColumns(null, null, null, null);
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        String[] columnName = new String[columnCount];
        for (int i = 1; i <= columnCount; i++) {
            // columnName[i - 1] = meta.getColumnLabel(i);
            columnName[i - 1] = meta.getColumnName(i);
        }
        while (rs.next()) {
            for (int i = 0; i < columnCount; i++) {
                rs.getObject(columnName[i]);
            }
        }
    }

    @Test
    void testFindColumn() throws SQLException {
        ResultSet rs;
        try {
            stmt.execute(DROPSTRING);
        } catch (Exception e) {
        }

        stmt.execute(CREATESTRING);
        rs = stmt.executeQuery(QUERYSTRING);
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(2, rs.findColumn("NAME"));
        assertEquals(3, rs.findColumn("CITY"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(2, rs.findColumn("name"));
        assertEquals(3, rs.findColumn("city"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(2, rs.findColumn("Name"));
        assertEquals(3, rs.findColumn("City"));
        stmt.execute(DROPSTRING);
    }

    @Test
    void testColumnLength() throws SQLException {
        ResultSet rs;
        ResultSetMetaData meta;

        try {
            stmt.execute(DROPSTRING);
        } catch (Exception e) {
        }

        stmt.execute(CREATESTRING);
        rs = stmt.executeQuery(QUERYSTRING);
        meta = rs.getMetaData();
        assertEquals("ID", meta.getColumnLabel(1).toUpperCase());
        assertEquals(11, meta.getColumnDisplaySize(1));
        assertEquals("NAME", meta.getColumnLabel(2).toUpperCase());
        assertEquals(40, meta.getColumnDisplaySize(2));
        stmt.execute(DROPSTRING);

        rs = stmt.executeQuery("select 1, 'Hello' union select 2, 'Hello World!'");
        meta = rs.getMetaData();
        assertEquals(11, meta.getColumnDisplaySize(1));
        //assertEquals(12, meta.getColumnDisplaySize(2));
    }

    @Test
    @Disabled("need to check SERIAL type in postgres")
    void testAutoIncrement() throws SQLException {
        stmt.execute("DROP TABLE IF EXISTS TEST");
        ResultSet rs;
        stmt.execute("CREATE TABLE TEST(ID IDENTITY NOT NULL, NAME VARCHAR NULL)");

        stmt.execute("INSERT INTO TEST(NAME) VALUES('Hello')",
                Statement.RETURN_GENERATED_KEYS);
        rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        stmt.execute("INSERT INTO TEST(NAME) VALUES('World')",
                Statement.RETURN_GENERATED_KEYS);
        rs = stmt.getGeneratedKeys();
        assertTrue(rs.next());
        assertEquals(2, rs.getInt(1));

        rs = stmt.executeQuery("SELECT ID AS I, NAME AS N, ID+1 AS IP1 FROM TEST");
        ResultSetMetaData meta = rs.getMetaData();
        assertTrue(meta.isAutoIncrement(1));
        assertFalse(meta.isAutoIncrement(2));
        assertFalse(meta.isAutoIncrement(3));
        assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
        assertEquals(ResultSetMetaData.columnNullable, meta.isNullable(2));
        assertEquals(ResultSetMetaData.columnNullableUnknown, meta.isNullable(3));
        assertTrue(rs.next());
        assertTrue(rs.next());
        assertFalse(rs.next());

    }

    @Test
    void testInt() throws SQLException {
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" INT)");
        stmt.execute("INSERT INTO TEST VALUES(1,-1)");
        stmt.execute("INSERT INTO TEST VALUES(2,0)");
        stmt.execute("INSERT INTO TEST VALUES(3,1)");
        stmt.execute("INSERT INTO TEST VALUES(4," + Integer.MAX_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(5," + Integer.MIN_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(6,NULL)");
        // this should not be read - maxrows=6
        stmt.execute("INSERT INTO TEST VALUES(7,NULL)");

        // MySQL compatibility (is this required?)
        // rs=stmt.executeQuery("SELECT * FROM TEST T ORDER BY ID");
        // check(rs.findColumn("T.ID"), 1);
        // check(rs.findColumn("T.NAME"), 2);

        rs = stmt.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        // MySQL compatibility
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(2, rs.findColumn("VALUE"));

        ResultSetMetaData meta = rs.getMetaData();
        assertEquals(3, meta.getColumnCount());
        //assertEquals("resultSet".toUpperCase(), meta.getCatalogName(1));
        //assertTrue("PUBLIC".equals(meta.getSchemaName(2)));
        assertTrue("TEST".equals(meta.getTableName(1).toUpperCase()));
        assertTrue("ID".equals(meta.getColumnName(1).toUpperCase()));
        assertTrue("VALUE".equals(meta.getColumnName(2).toUpperCase()));
        assertFalse(meta.isAutoIncrement(1));
        assertFalse(meta.isCaseSensitive(1));
        assertTrue(meta.isSearchable(1));
        assertFalse(meta.isCurrency(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.isSigned(1));
        assertTrue(meta.isSearchable(2));
        assertEquals(ResultSetMetaData.columnNoNulls, meta.isNullable(1));
        assertFalse(meta.isReadOnly(1));
        assertTrue(meta.isWritable(1));
        assertFalse(meta.isDefinitelyWritable(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertTrue(meta.getColumnDisplaySize(2) > 0);
        //assertEquals(Void.class.getName(), meta.getColumnClassName(3));

        //assertTrue(rs.getRow() == 0);
        /*assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
                new int[] { Types.INTEGER, Types.INTEGER,
                        Types.NULL }, new int[] { 32, 32, 1 }, new int[] { 0, 0, 0 });*/
        rs.next();
        assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
        int defaultFetchSize = rs.getFetchSize();
        // 0 should be an allowed value (but it's not defined what is actually
        // means)
        rs.setFetchSize(0);
        //assertThrows(ErrorCode.INVALID_VALUE_2, rs).setFetchSize(-1);
        // fetch size 100 is bigger than maxrows - not allowed
        //assertThrows(ErrorCode.INVALID_VALUE_2, rs).setFetchSize(100);
        rs.setFetchSize(6);

        //assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        assertEquals(2, rs.findColumn("value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(2, rs.findColumn("Value"));
        assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        assertEquals(1, rs.findColumn("Id"));
        assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getInt(2) == -1 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == -1 && !rs.wasNull());
        //assertTrue(rs.getInt("value") == -1 && !rs.wasNull());
        //assertTrue(rs.getInt("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());

        //o = rs.getObject("value");
        o = rs.getInt("VALUE");

        assertTrue(o instanceof Integer);
        assertTrue((Integer) o == -1);
        //o = rs.getObject("value", Integer.class);
        //assertTrue(o instanceof Integer);
        //assertTrue((Integer) o == -1);
        //o = rs.getObject(2);
        o = rs.getInt(2);
        assertTrue(o instanceof Integer);
        assertTrue((Integer) o == -1);
        //o = rs.getObject(2, Integer.class);
        //assertTrue(o instanceof Integer);
        //assertTrue((Integer) o == -1);
        //assertTrue(rs.getBoolean("VALUE"));
        //assertTrue(rs.getByte("VALUE") == (byte) -1);
        assertTrue(rs.getShort("VALUE") == (short) -1);
        assertTrue(rs.getLong("VALUE") == -1);
        assertTrue(rs.getFloat("VALUE") == -1.0);
        assertTrue(rs.getDouble("VALUE") == -1.0);

        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());
        //assertTrue(rs.getInt("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getInt("id") == 1 && !rs.wasNull());
        //assertTrue(rs.getInt("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        //assertTrue(rs.getRow() == 2);
        assertTrue(rs.getInt(2) == 0 && !rs.wasNull());
        assertFalse(rs.getBoolean(2));
        //assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getShort(2) == 0);
        assertTrue(rs.getLong(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        //assertTrue(rs.getRow() == 3);
        assertTrue(rs.getInt("id") == 3 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == 1 && !rs.wasNull());
        rs.next();
        //assertTrue(rs.getRow() == 4);
        assertTrue(rs.getInt("id") == 4 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == Integer.MAX_VALUE && !rs.wasNull());
        rs.next();
        //assertTrue(rs.getRow() == 5);
        assertTrue(rs.getInt("id") == 5 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == Integer.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();
        //assertTrue(rs.getRow() == 6);
        assertTrue(rs.getInt("id") == 6 && !rs.wasNull());
        assertTrue(rs.getInt("VALUE") == 0 && rs.wasNull());
        assertTrue(rs.getInt(2) == 0 && rs.wasNull());
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        o = rs.getObject(2);
        assertNull(o);
        assertTrue(rs.wasNull());
        //o = rs.getObject(2, Integer.class);
        //assertNull(o);
        //assertTrue(rs.wasNull());
        assertFalse(rs.next());
        //assertEquals(0, rs.getRow());
        // there is one more row, but because of setMaxRows we don't get it

        stmt.execute("DROP TABLE TEST");
        stmt.setMaxRows(0);
    }

    @Test
    void testSmallInt() throws SQLException {
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" SMALLINT)");
        stmt.execute("INSERT INTO TEST VALUES(1,-1)");
        stmt.execute("INSERT INTO TEST VALUES(2,0)");
        stmt.execute("INSERT INTO TEST VALUES(3,1)");
        stmt.execute("INSERT INTO TEST VALUES(4," + Short.MAX_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(5," + Short.MIN_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(6,NULL)");

        rs = stmt.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        //assertTrue(rs.getRow() == 0);
        //assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
        //new int[]{Types.INTEGER, Types.SMALLINT,
        //        Types.NULL}, new int[]{32, 16, 1}, new int[]{0, 0, 0});
        rs.next();

        //assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        //assertEquals(2, rs.findColumn("value"));
        //assertEquals(2, rs.findColumn("Value"));
        //assertEquals(2, rs.findColumn("Value"));
        //assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        //assertEquals(1, rs.findColumn("Id"));
        //assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getShort(2) == -1 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == -1 && !rs.wasNull());
        //assertTrue(rs.getShort("value") == -1 && !rs.wasNull());
        //assertTrue(rs.getShort("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());

        //o = rs.getObject("value");
        o = rs.getInt("VALUE");
        assertTrue(o.getClass() == Integer.class);
        assertTrue(((Number) o).intValue() == -1);
        //o = rs.getObject("value", Short.class);
        o = rs.getShort("VALUE");
        assertTrue(o instanceof Short);
        assertTrue((Short) o == -1);
        //o = rs.getObject(2);
        o = rs.getInt(2);
        assertTrue(o.getClass() == Integer.class);
        assertTrue(((Number) o).intValue() == -1);
        //o = rs.getObject(2, Short.class);
        o = rs.getShort(2);
        assertTrue(o instanceof Short);
        assertTrue((Short) o == -1);
        //assertTrue(rs.getBoolean("VALUE"));
        //assertTrue(rs.getByte("Value") == (byte) -1);
        assertTrue(rs.getInt("VALUE") == -1);
        assertTrue(rs.getLong("VALUE") == -1);
        assertTrue(rs.getFloat("VALUE") == -1.0);
        assertTrue(rs.getDouble("VALUE") == -1.0);

        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());
        //assertTrue(rs.getShort("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getShort("id") == 1 && !rs.wasNull());
        //assertTrue(rs.getShort("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getShort(1) == 1 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 2);
        assertTrue(rs.getShort(2) == 0 && !rs.wasNull());
        assertFalse(rs.getBoolean(2));
        //assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getInt(2) == 0);
        assertTrue(rs.getLong(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getShort(1) == 2 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 3);
        assertTrue(rs.getShort("id") == 3 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == 1 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 4);
        assertTrue(rs.getShort("id") == 4 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == Short.MAX_VALUE && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 5);
        assertTrue(rs.getShort("id") == 5 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == Short.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 6);
        assertTrue(rs.getShort("id") == 6 && !rs.wasNull());
        assertTrue(rs.getShort("VALUE") == 0 && rs.wasNull());
        assertTrue(rs.getShort(2) == 0 && rs.wasNull());
        assertTrue(rs.getShort(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        //o = rs.getObject(2);
        //assertNull(o);
        //assertTrue(rs.wasNull());
        //o = rs.getObject(2, Short.class);
        //assertNull(o);
        //assertTrue(rs.wasNull());
        //assertFalse(rs.next());
        //assertEquals(0, rs.getRow());

        stmt.execute("DROP TABLE TEST");
        stmt.setMaxRows(0);
    }

    @Test
    void testBigInt() throws SQLException {
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" BIGINT)");
        stmt.execute("INSERT INTO TEST VALUES(1,-1)");
        stmt.execute("INSERT INTO TEST VALUES(2,0)");
        stmt.execute("INSERT INTO TEST VALUES(3,1)");
        stmt.execute("INSERT INTO TEST VALUES(4," + Long.MAX_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(5," + Long.MIN_VALUE + ")");
        stmt.execute("INSERT INTO TEST VALUES(6,NULL)");

        rs = stmt.executeQuery("SELECT *, NULL AS N FROM TEST ORDER BY ID");

        //assertTrue(rs.getRow() == 0);
        //assertResultSetMeta(rs, 3, new String[] { "ID", "VALUE", "N" },
        //new int[]{Types.INTEGER, Types.BIGINT,
        //        Types.NULL}, new int[]{32, 64, 1}, new int[]{0, 0, 0});
        rs.next();

        //assertTrue(rs.getRow() == 1);
        assertEquals(2, rs.findColumn("VALUE"));
        //assertEquals(2, rs.findColumn("value"));
        //assertEquals(2, rs.findColumn("Value"));
        //assertEquals(1, rs.findColumn("ID"));
        assertEquals(1, rs.findColumn("id"));
        //assertEquals(1, rs.findColumn("Id"));
        //assertEquals(1, rs.findColumn("iD"));
        assertTrue(rs.getLong(2) == -1 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == -1 && !rs.wasNull());
        //assertTrue(rs.getLong("value") == -1 && !rs.wasNull());
        //assertTrue(rs.getLong("Value") == -1 && !rs.wasNull());
        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());

        //o = rs.getObject("value");
        o = rs.getLong("VALUE");
        assertTrue(o instanceof Long);
        assertTrue((Long) o == -1);
        //o = rs.getObject("value", Long.class);
        //assertTrue(o instanceof Long);
        //assertTrue((Long) o == -1);
        //o = rs.getObject("value", BigInteger.class);
        o = rs.getBigDecimal("VALUE");
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).longValue() == -1);
        //o = rs.getObject(2);
        //assertTrue(o instanceof Long);
        //assertTrue((Long) o == -1);
        //o = rs.getObject(2, Long.class);
        o = rs.getLong(2);
        assertTrue(o instanceof Long);
        assertTrue((Long) o == -1);
        //o = rs.getObject(2, BigInteger.class);
        o = rs.getBigDecimal(2);
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).longValue() == -1);
        //assertTrue(rs.getBoolean("VALUE"));
        //assertTrue(rs.getByte("VALUE") == (byte) -1);
        assertTrue(rs.getShort("VALUE") == -1);
        assertTrue(rs.getInt("VALUE") == -1);
        assertTrue(rs.getFloat("VALUE") == -1.0);
        assertTrue(rs.getDouble("VALUE") == -1.0);

        assertTrue(rs.getString("VALUE").equals("-1") && !rs.wasNull());
        //assertTrue(rs.getLong("ID") == 1 && !rs.wasNull());
        assertTrue(rs.getLong("id") == 1 && !rs.wasNull());
        //assertTrue(rs.getLong("Id") == 1 && !rs.wasNull());
        assertTrue(rs.getLong(1) == 1 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 2);
        assertTrue(rs.getLong(2) == 0 && !rs.wasNull());
        assertFalse(rs.getBoolean(2));
        //assertTrue(rs.getByte(2) == 0);
        assertTrue(rs.getShort(2) == 0);
        assertTrue(rs.getInt(2) == 0);
        assertTrue(rs.getFloat(2) == 0.0);
        assertTrue(rs.getDouble(2) == 0.0);
        assertTrue(rs.getString(2).equals("0") && !rs.wasNull());
        assertTrue(rs.getLong(1) == 2 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 3);
        assertTrue(rs.getLong("id") == 3 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == 1 && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 4);
        assertTrue(rs.getLong("id") == 4 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == Long.MAX_VALUE && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 5);
        assertTrue(rs.getLong("id") == 5 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == Long.MIN_VALUE && !rs.wasNull());
        assertTrue(rs.getString(1).equals("5") && !rs.wasNull());
        rs.next();

        //assertTrue(rs.getRow() == 6);
        assertTrue(rs.getLong("id") == 6 && !rs.wasNull());
        assertTrue(rs.getLong("VALUE") == 0 && rs.wasNull());
        assertTrue(rs.getLong(2) == 0 && rs.wasNull());
        assertTrue(rs.getLong(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(1).equals("6") && !rs.wasNull());
        assertTrue(rs.getString(2) == null && rs.wasNull());
        //o = rs.getObject(2);
        //assertNull(o);
        //assertTrue(rs.wasNull());
        //o = rs.getObject(2, Long.class);
        //assertNull(o);
        //assertTrue(rs.wasNull());
        assertFalse(rs.next());
        //assertEquals(0, rs.getRow());

        stmt.execute("DROP TABLE TEST");
        stmt.setMaxRows(0);
    }

    @Test
    void testVarchar() throws SQLException {
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" VARCHAR(255))");
        stmt.execute("INSERT INTO TEST VALUES(1,'')");
        stmt.execute("INSERT INTO TEST VALUES(2,' ')");
        stmt.execute("INSERT INTO TEST VALUES(3,'  ')");
        stmt.execute("INSERT INTO TEST VALUES(4,NULL)");
        stmt.execute("INSERT INTO TEST VALUES(5,'Hi')");
        stmt.execute("INSERT INTO TEST VALUES(6,' Hi ')");
        stmt.execute("INSERT INTO TEST VALUES(7,'Joe''s')");
        stmt.execute("INSERT INTO TEST VALUES(8,'{escape}')");
        stmt.execute("INSERT INTO TEST VALUES(9,'\\n')");
        stmt.execute("INSERT INTO TEST VALUES(10,'\\''')");
        stmt.execute("INSERT INTO TEST VALUES(11,'\\%')");
        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        //assertResultSetMeta(rs, 2, new String[]{"ID", "VALUE"},
        //        new int[]{Types.INTEGER, Types.VARCHAR}, new int[]{
        //                32, 255}, new int[]{0, 0});
        String value;
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <>)");
        assertTrue(value != null && value.equals("") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 1 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: < >)");
        assertTrue(rs.getString(2).equals(" ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 2 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <  >)");
        assertTrue(rs.getString(2).equals("  ") && !rs.wasNull());
        assertTrue(rs.getInt(1) == 3 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <null>)");
        assertTrue(rs.getString(2) == null && rs.wasNull());
        assertTrue(rs.getInt(1) == 4 && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <Hi>)");
        assertTrue(rs.getInt(1) == 5 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Hi") && !rs.wasNull());
        o = rs.getObject("VALUE");
        //trace(o.getClass().getName());
        assertTrue(o instanceof String);
        assertTrue(o.toString().equals("Hi"));
        //o = rs.getObject("value", String.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof String);
        //assertTrue(o.equals("Hi"));
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: < Hi >)");
        assertTrue(rs.getInt(1) == 6 && !rs.wasNull());
        assertTrue(rs.getString(2).equals(" Hi ") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <Joe's>)");
        assertTrue(rs.getInt(1) == 7 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("Joe's") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <{escape}>)");
        assertTrue(rs.getInt(1) == 8 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("{escape}") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <\\n>)");
        assertTrue(rs.getInt(1) == 9 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\n") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <\\'>)");
        assertTrue(rs.getInt(1) == 10 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\'") && !rs.wasNull());
        rs.next();
        value = rs.getString(2);
        //trace("Value: <" + value + "> (should be: <\\%>)");
        assertTrue(rs.getInt(1) == 11 && !rs.wasNull());
        assertTrue(rs.getString(2).equals("\\%") && !rs.wasNull());
        assertFalse(rs.next());
        stmt.execute("DROP TABLE TEST");
    }

    @Test
    void testDecimal() throws SQLException {
        //trace("Test DECIMAL");
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" DECIMAL(10,2))");
        stmt.execute("INSERT INTO TEST VALUES(1,-1)");
        stmt.execute("INSERT INTO TEST VALUES(2,.0)");
        stmt.execute("INSERT INTO TEST VALUES(3,1.)");
        stmt.execute("INSERT INTO TEST VALUES(4,12345678.89)");
        stmt.execute("INSERT INTO TEST VALUES(6,99999998.99)");
        stmt.execute("INSERT INTO TEST VALUES(7,-99999998.99)");
        stmt.execute("INSERT INTO TEST VALUES(8,NULL)");
        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        //assertResultSetMeta(rs, 2, new String[] { "ID", "VALUE" },
        //        new int[] { Types.INTEGER, Types.DECIMAL }, new int[] {
        //                32, 10 }, new int[] { 0, 2 });
        BigDecimal bd;

        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertFalse(rs.wasNull());
//        assertTrue(rs.getInt(2) == -1);
        //assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertFalse(rs.wasNull());
        //o = rs.getObject(2);
        //trace(o.getClass().getName());
        o = rs.getBigDecimal(2);
        assertTrue(o instanceof BigDecimal);
        assertTrue(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);
        //o = rs.getObject(2, BigDecimal.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof BigDecimal);
        //assertTrue(((BigDecimal) o).compareTo(new BigDecimal("-1.00")) == 0);

        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertFalse(rs.wasNull());
        //assertTrue(rs.getInt(2) == 0);
        //assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertFalse(rs.wasNull());

        rs.next();
        //checkColumnBigDecimal(rs, 2, 1, "1.00");

        rs.next();
        //checkColumnBigDecimal(rs, 2, 12345679, "12345678.89");

        rs.next();
        //checkColumnBigDecimal(rs, 2, 99999999, "99999998.99");

        rs.next();
        //checkColumnBigDecimal(rs, 2, -99999999, "-99999998.99");

        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);

        assertFalse(rs.next());
        stmt.execute("DROP TABLE TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" DECIMAL(22,2))");
        stmt.execute("INSERT INTO TEST VALUES(1,-12345678909876543210)");
        stmt.execute("INSERT INTO TEST VALUES(2,12345678901234567890.12345)");
        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        rs.next();
        assertEquals(new BigDecimal("-12345678909876543210.00"), rs.getBigDecimal(2));
        //assertEquals(new BigInteger("-12345678909876543210"), rs.getObject(2, BigInteger.class));
        rs.next();
        assertEquals(new BigDecimal("12345678901234567890.12"), rs.getBigDecimal(2));
        //assertEquals(new BigInteger("12345678901234567890"), rs.getObject(2, BigInteger.class));
        stmt.execute("DROP TABLE TEST");
    }

    void checkColumnBigDecimal(ResultSet rs, int column, int i,
                               String bd) throws SQLException {
        BigDecimal bd1 = rs.getBigDecimal(column);
        int i1 = rs.getInt(column);
        if (bd == null) {
            //trace("should be: null");
            assertTrue(rs.wasNull());
        } else {
            //trace("BigDecimal i=" + i + " bd=" + bd + " ; i1=" + i1 + " bd1=" + bd1);
            assertFalse(rs.wasNull());
            assertTrue(i1 == i);
            assertTrue(bd1.compareTo(new BigDecimal(bd)) == 0);
        }
    }

    @Test
    void testDoubleFloat() throws SQLException {
        //trace("Test DOUBLE - FLOAT");
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, D DOUBLE, R REAL, F DECFLOAT)");
        stmt.execute("INSERT INTO TEST VALUES(1, -1, -1, -1)");
        stmt.execute("INSERT INTO TEST VALUES(2, .0, .0, .0)");
        stmt.execute("INSERT INTO TEST VALUES(3, 1., 1., 1.)");
        stmt.execute("INSERT INTO TEST VALUES(4, 12345678.89, 12345678.89, 12345678.89)");
        stmt.execute("INSERT INTO TEST VALUES(6, 99999999.99, 99999999.99, 99999999.99)");
        stmt.execute("INSERT INTO TEST VALUES(7, -99999999.99, -99999999.99, -99999999.99)");
        stmt.execute("INSERT INTO TEST VALUES(8, NULL, NULL, NULL)");
        stmt.execute("INSERT INTO TEST VALUES(9, '-Infinity', '-Infinity', '-Infinity')");
        stmt.execute("INSERT INTO TEST VALUES(10, 'Infinity', 'Infinity', 'Infinity')");
        stmt.execute("INSERT INTO TEST VALUES(11, 'NaN', 'NaN', 'NaN')");
        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        //assertResultSetMeta(rs, 4, new String[]{"ID", "D", "R", "F"},
        //        null,
        //        new int[]{32, 53, 24, 100_000}, new int[]{0, 0, 0, 0});
        ResultSetMetaData md = rs.getMetaData();
        assertEquals("INTEGER", md.getColumnTypeName(1));
        assertEquals("DOUBLE PRECISION", md.getColumnTypeName(2));
        assertEquals("REAL", md.getColumnTypeName(3));
        assertEquals("DECFLOAT", md.getColumnTypeName(4));
        BigDecimal bd;
        rs.next();
        assertTrue(rs.getInt(1) == 1);
        assertFalse(rs.wasNull());
        assertTrue(rs.getInt(2) == -1);
        assertTrue(rs.getInt(3) == -1);
        assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("-1.00")) == 0);
        assertFalse(rs.wasNull());
        //o = rs.getObject(2);
        //trace(o.getClass().getName());
        o = rs.getDouble(2);
        assertTrue(o instanceof Double);
        assertTrue(((Double) o).compareTo(-1d) == 0);
        //o = rs.getObject(2, Double.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof Double);
        assertTrue(((Double) o).compareTo(-1d) == 0);
        //o = rs.getObject(3);
        //trace(o.getClass().getName());
        o = rs.getFloat(3);
        assertTrue(o instanceof Float);
        assertTrue(((Float) o).compareTo(-1f) == 0);
        //o = rs.getObject(3, Float.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof Float);
        //assertTrue(((Float) o).compareTo(-1f) == 0);
        //o = rs.getObject(4);
        //trace(o.getClass().getName());
        o = rs.getBigDecimal(4);
        assertTrue(o instanceof BigDecimal);
        assertEquals(BigDecimal.valueOf(-1L, 0), o);
        //o = rs.getObject(4, BigDecimal.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof BigDecimal);
        //assertEquals(BigDecimal.valueOf(-1L, 0), o);
        rs.next();
        assertTrue(rs.getInt(1) == 2);
        assertFalse(rs.wasNull());
        assertTrue(rs.getInt(2) == 0);
        assertFalse(rs.wasNull());
        assertTrue(rs.getInt(3) == 0);
        assertFalse(rs.wasNull());
        assertTrue(rs.getInt(4) == 0);
        assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(2);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(3);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertFalse(rs.wasNull());
        bd = rs.getBigDecimal(4);
        assertTrue(bd.compareTo(new BigDecimal("0.00")) == 0);
        assertFalse(rs.wasNull());
        rs.next();
        assertEquals(1.0, rs.getDouble(2));
        assertEquals(1.0f, rs.getFloat(3));
        assertEquals(BigDecimal.ONE, rs.getBigDecimal(4));
        rs.next();
        assertEquals(12345678.89, rs.getDouble(2));
        assertEquals(12345678.89f, rs.getFloat(3));
        assertEquals(BigDecimal.valueOf(12_345_678_89L, 2), rs.getBigDecimal(4));
        rs.next();
        assertEquals(99999999.99, rs.getDouble(2));
        assertEquals(99999999.99f, rs.getFloat(3));
        assertEquals(BigDecimal.valueOf(99_999_999_99L, 2), rs.getBigDecimal(4));
        rs.next();
        assertEquals(-99999999.99, rs.getDouble(2));
        assertEquals(-99999999.99f, rs.getFloat(3));
        assertEquals(BigDecimal.valueOf(-99_999_999_99L, 2), rs.getBigDecimal(4));
        rs.next();
        checkColumnBigDecimal(rs, 2, 0, null);
        checkColumnBigDecimal(rs, 3, 0, null);
        checkColumnBigDecimal(rs, 4, 0, null);
        rs.next();
        assertEquals(Float.NEGATIVE_INFINITY, rs.getFloat(2));
        assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble(3));
        assertThrows(SQLException.class, () -> rs.getBigDecimal(4));
        assertThrows(SQLException.class, () -> rs.getObject(4));
        assertEquals(Double.NEGATIVE_INFINITY, rs.getDouble(4));
        assertEquals("-Infinity", rs.getString(4));
        rs.next();
        assertEquals(Float.POSITIVE_INFINITY, rs.getFloat(2));
        assertEquals(Double.POSITIVE_INFINITY, rs.getDouble(3));
        assertThrows(SQLException.class, () -> rs.getBigDecimal(4));
        assertThrows(SQLException.class, () -> rs.getObject(4));
        assertEquals(Double.POSITIVE_INFINITY, rs.getDouble(4));
        assertEquals("Infinity", rs.getString(4));
        rs.next();
        assertEquals(Float.NaN, rs.getFloat(2));
        assertEquals(Double.NaN, rs.getDouble(3));
        assertThrows(SQLException.class, () -> rs.getBigDecimal(4));
        assertThrows(SQLException.class, () -> rs.getObject(4));
        assertEquals(Double.NaN, rs.getDouble(4));
        assertEquals("NaN", rs.getString(4));
        assertFalse(rs.next());
        stmt.execute("DROP TABLE TEST");
    }

    @Test
    void testDatetime() throws SQLException {
        //trace("Test DATETIME");
        ResultSet rs;
        Object o;

        stmt.execute("DROP TABLE IF EXISTS TEST");

        rs = stmt.executeQuery("select date '99999-12-23'");
        rs.next();
        assertEquals("99999-12-23", rs.getString(1));
        rs = stmt.executeQuery("select timestamp '99999-12-23 01:02:03.000'");
        rs.next();
        assertEquals("99999-12-23 01:02:03", rs.getString(1));
        rs = stmt.executeQuery("select date '-99999-12-23'");
        rs.next();
        assertEquals("-99999-12-23", rs.getString(1));
        rs = stmt.executeQuery("select timestamp '-99999-12-23 01:02:03.000'");
        rs.next();
        assertEquals("-99999-12-23 01:02:03", rs.getString(1));

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY,\"VALUE\" DATETIME)");
        stmt.execute("INSERT INTO TEST VALUES(1,DATE '2011-11-11')");
        stmt.execute("INSERT INTO TEST VALUES(2,TIMESTAMP '2002-02-02 02:02:02')");
        stmt.execute("INSERT INTO TEST VALUES(3,TIMESTAMP '1800-1-1 0:0:0')");
        stmt.execute("INSERT INTO TEST VALUES(4,TIMESTAMP '9999-12-31 23:59:59')");
        stmt.execute("INSERT INTO TEST VALUES(5,NULL)");
        rs = stmt.executeQuery("SELECT 0 ID, " +
                "TIMESTAMP '9999-12-31 23:59:59' \"VALUE\" FROM TEST ORDER BY ID");
        //assertResultSetMeta(rs, 2, new String[]{"ID", "VALUE"},
        //        new int[]{Types.INTEGER, Types.TIMESTAMP},
        //        new int[]{32, 29}, new int[]{0, 9});
        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        //assertResultSetMeta(rs, 2, new String[]{"ID", "VALUE"},
        //        new int[]{Types.INTEGER, Types.TIMESTAMP},
        //        new int[]{32, 26}, new int[]{0, 6});
        rs.next();
        java.sql.Date date;
        java.sql.Time time;
        java.sql.Timestamp ts;
        date = rs.getDate(2);
        assertFalse(rs.wasNull());
        time = rs.getTime(2);
        assertFalse(rs.wasNull());
        ts = rs.getTimestamp(2);
        assertFalse(rs.wasNull());
        //trace("Date: " + date.toString() + " Time:" + time.toString() +
        //        " Timestamp:" + ts.toString());
        //trace("Date ms: " + date.getTime() + " Time ms:" + time.getTime() +
        //        " Timestamp ms:" + ts.getTime());
        //trace("1970 ms: " + java.sql.Timestamp.valueOf(
        //        "1970-01-01 00:00:00.0").getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "2011-11-11 00:00:00.0").getTime(), date.getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "1970-01-01 00:00:00.0").getTime(), time.getTime());
        assertEquals(java.sql.Timestamp.valueOf(
                "2011-11-11 00:00:00.0").getTime(), ts.getTime());
        assertTrue(date.equals(
                java.sql.Date.valueOf("2011-11-11")));
        assertTrue(time.equals(
                java.sql.Time.valueOf("00:00:00")));
        assertTrue(ts.equals(
                java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        //o = rs.getObject(2);
        //trace(o.getClass().getName());
        o = rs.getTimestamp(2);
        assertTrue(o instanceof java.sql.Timestamp);
        assertTrue(((java.sql.Timestamp) o).equals(
                java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        assertFalse(rs.wasNull());
        //o = rs.getObject(2, java.sql.Timestamp.class);
        //trace(o.getClass().getName());
        //assertTrue(o instanceof java.sql.Timestamp);
        //assertTrue(((java.sql.Timestamp) o).equals(
        //        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0")));
        //assertFalse(rs.wasNull());
        //o = rs.getObject(2, java.util.Date.class);
        //assertTrue(o.getClass() == java.util.Date.class);
        //assertEquals(((java.util.Date) o).getTime(),
        //        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        //o = rs.getObject(2, Calendar.class);
        //assertTrue(o instanceof Calendar);
        //assertEquals(((Calendar) o).getTimeInMillis(),
        //        java.sql.Timestamp.valueOf("2011-11-11 00:00:00.0").getTime());
        rs.next();

        date = rs.getDate("VALUE");
        assertFalse(rs.wasNull());
        time = rs.getTime("VALUE");
        assertFalse(rs.wasNull());
        ts = rs.getTimestamp("VALUE");
        assertFalse(rs.wasNull());
        //trace("Date: " + date.toString() +
        //        " Time:" + time.toString() + " Timestamp:" + ts.toString());
        assertEquals("2002-02-02", date.toString());
        assertEquals("02:02:02", time.toString());
        assertEquals("2002-02-02 02:02:02.0", ts.toString());
        rs.next();

        //assertEquals("1800-01-01", rs.getObject("value", LocalDate.class).toString());
        assertEquals("00:00:00", rs.getTime("VALUE").toString());
        //assertEquals("00:00", rs.getObject("value", LocalTime.class).toString());
        //assertEquals("1800-01-01T00:00", rs.getObject("value", LocalDateTime.class).toString());
        rs.next();

        assertEquals("9999-12-31", rs.getDate("VALUE").toString());
        //assertEquals("9999-12-31", rs.getObject("Value", LocalDate.class).toString());
        assertEquals("23:59:59", rs.getTime("VALUE").toString());
        //assertEquals("23:59:59", rs.getObject("Value", LocalTime.class).toString());
        assertEquals("9999-12-31 23:59:59.0", rs.getTimestamp("VALUE").toString());
        //assertEquals("9999-12-31T23:59:59", rs.getObject("Value", LocalDateTime.class).toString());
        rs.next();

        assertTrue(rs.getDate("VALUE") == null && rs.wasNull());
        assertTrue(rs.getTime("VALUE") == null && rs.wasNull());
        assertTrue(rs.getTimestamp(2) == null && rs.wasNull());
        //assertTrue(rs.getObject(2, LocalDateTime.class) == null && rs.wasNull());
        assertFalse(rs.next());

        rs = stmt.executeQuery("SELECT DATE '2001-02-03' D, " +
                "TIME '14:15:16', " +
                "TIMESTAMP '2007-08-09 10:11:12.141516171' TS FROM TEST");
        rs.next();

        date = (Date) rs.getObject(1);
        time = (Time) rs.getObject(2);
        ts = (Timestamp) rs.getObject(3);
        assertEquals("2001-02-03", date.toString());
        assertEquals("14:15:16", time.toString());
        assertEquals("2007-08-09 10:11:12.141516171", ts.toString());
        //date = rs.getObject(1, Date.class);
        date = rs.getDate(1);
        //time = rs.getObject(2, Time.class);
        time = rs.getTime(2);
        //ts = rs.getObject(3, Timestamp.class);
        ts = rs.getTimestamp(3);
        assertEquals("2001-02-03", date.toString());
        assertEquals("14:15:16", time.toString());
        assertEquals("2007-08-09 10:11:12.141516171", ts.toString());
        //assertEquals("2001-02-03", rs.getObject(1, LocalDate.class).toString());
        //assertEquals("14:15:16", rs.getObject(2, LocalTime.class).toString());
        //assertEquals("2007-08-09T10:11:12.141516171", rs.getObject(3, LocalDateTime.class).toString());

        stmt.execute("DROP TABLE TEST");

        rs = stmt.executeQuery("SELECT LOCALTIME, CURRENT_TIME");
        rs.next();
        assertEquals(rs.getTime(1), rs.getTime(2));
        rs = stmt.executeQuery("SELECT LOCALTIMESTAMP, CURRENT_TIMESTAMP");
        rs.next();
        assertEquals(rs.getTimestamp(1), rs.getTimestamp(2));

        rs = stmt.executeQuery("SELECT DATE '-1000000000-01-01', " + "DATE '1000000000-12-31'");
        rs.next();
        //assertEquals("-999999999-01-01", rs.getObject(1, LocalDate.class).toString());
        //assertEquals("+999999999-12-31", rs.getObject(2, LocalDate.class).toString());

        rs = stmt.executeQuery("SELECT TIMESTAMP '-1000000000-01-01 00:00:00', "
                + "TIMESTAMP '1000000000-12-31 23:59:59.999999999'");
        rs.next();
        //assertEquals("-999999999-01-01T00:00", rs.getObject(1, LocalDateTime.class).toString());
        //assertEquals("+999999999-12-31T23:59:59.999999999", rs.getObject(2, LocalDateTime.class).toString());

        rs = stmt.executeQuery("SELECT TIMESTAMP WITH TIME ZONE '-1000000000-01-01 00:00:00Z', "
                + "TIMESTAMP WITH TIME ZONE '1000000000-12-31 23:59:59.999999999Z', "
                + "TIMESTAMP WITH TIME ZONE '-1000000000-01-01 00:00:00+18', "
                + "TIMESTAMP WITH TIME ZONE '1000000000-12-31 23:59:59.999999999-18'");
        rs.next();
        //assertEquals("-999999999-01-01T00:00Z", rs.getObject(1, OffsetDateTime.class).toString());
        //assertEquals("+999999999-12-31T23:59:59.999999999Z", rs.getObject(2, OffsetDateTime.class).toString());
        //assertEquals("-999999999-01-01T00:00+18:00", rs.getObject(3, OffsetDateTime.class).toString());
        //assertEquals("+999999999-12-31T23:59:59.999999999-18:00", rs.getObject(4, OffsetDateTime.class).toString());
        //assertEquals("-999999999-01-01T00:00Z", rs.getObject(1, ZonedDateTime.class).toString());
        //assertEquals("+999999999-12-31T23:59:59.999999999Z", rs.getObject(2, ZonedDateTime.class).toString());
        //assertEquals("-999999999-01-01T00:00+18:00", rs.getObject(3, ZonedDateTime.class).toString());
        //assertEquals("+999999999-12-31T23:59:59.999999999-18:00", rs.getObject(4, ZonedDateTime.class).toString());
        //assertEquals("-1000000000-01-01T00:00:00Z", rs.getObject(1, Instant.class).toString());
        //assertEquals("+1000000000-12-31T23:59:59.999999999Z", rs.getObject(2, Instant.class).toString());
        //assertEquals("-1000000000-01-01T00:00:00Z", rs.getObject(3, Instant.class).toString());
        //assertEquals("+1000000000-12-31T23:59:59.999999999Z", rs.getObject(4, Instant.class).toString());

        rs = stmt.executeQuery("SELECT LOCALTIME, CURRENT_TIME");
        rs.next();
        //assertEquals(rs.getObject(1, LocalTime.class), rs.getObject(2, LocalTime.class));
        //assertEquals(rs.getObject(1, OffsetTime.class), rs.getObject(2, OffsetTime.class));
        rs = stmt.executeQuery("SELECT LOCALTIMESTAMP, CURRENT_TIMESTAMP");
        rs.next();
        //assertEquals(rs.getObject(1, LocalDateTime.class), rs.getObject(2, LocalDateTime.class));
        //assertEquals(rs.getObject(1, OffsetDateTime.class), rs.getObject(2, OffsetDateTime.class));
    }

    @Test
    @Disabled("Not implemented yet")
    void testDatetimeWithCalendar() throws SQLException {
        /*trace("Test DATETIME with Calendar");
        ResultSet rs;

        stmt.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, " +
                "D DATE, T TIME, TS TIMESTAMP(9))");
        PreparedStatement prep = conn.prepareStatement(
                "INSERT INTO TEST VALUES(?, ?, ?, ?)");
        GregorianCalendar regular = new GregorianCalendar();
        GregorianCalendar other = null;
        // search a locale that has a _different_ raw offset
        long testTime = java.sql.Date.valueOf("2001-02-03").getTime();
        for (String s : TimeZone.getAvailableIDs()) {
            TimeZone zone = TimeZone.getTimeZone(s);
            long rawOffsetDiff = regular.getTimeZone().getRawOffset() -
                    zone.getRawOffset();
            // must not be the same timezone (not 0 h and not 24 h difference
            // as for Pacific/Auckland and Etc/GMT+12)
            if (rawOffsetDiff != 0 && rawOffsetDiff != 1000 * 60 * 60 * 24) {
                if (regular.getTimeZone().getOffset(testTime) !=
                        zone.getOffset(testTime)) {
                    other = new GregorianCalendar(zone);
                    break;
                }
            }
        }

        trace("regular offset = " + regular.getTimeZone().getRawOffset() +
                " other = " + other.getTimeZone().getRawOffset());

        prep.setInt(1, 0);
        prep.setDate(2, null, regular);
        prep.setTime(3, null, regular);
        prep.setTimestamp(4, null, regular);
        prep.execute();

        prep.setInt(1, 1);
        prep.setDate(2, null, other);
        prep.setTime(3, null, other);
        prep.setTimestamp(4, null, other);
        prep.execute();

        prep.setInt(1, 2);
        prep.setDate(2, java.sql.Date.valueOf("2001-02-03"), regular);
        prep.setTime(3, java.sql.Time.valueOf("04:05:06"), regular);
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2007-08-09 10:11:12.131415"), regular);
        prep.execute();

        prep.setInt(1, 3);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"), other);
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"), other);
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"), other);
        prep.execute();

        prep.setInt(1, 4);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"));
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"));
        prep.setTimestamp(4,
                java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"));
        prep.execute();

        prep.setInt(1, 5);
        prep.setDate(2, java.sql.Date.valueOf("2101-02-03"), null);
        prep.setTime(3, java.sql.Time.valueOf("14:05:06"), null);
        prep.setTimestamp(4, java.sql.Timestamp.valueOf("2107-08-09 10:11:12.131415"), null);
        prep.execute();

        rs = stmt.executeQuery("SELECT * FROM TEST ORDER BY ID");
        assertResultSetMeta(rs, 4,
                new String[]{"ID", "D", "T", "TS"},
                new int[]{Types.INTEGER, Types.DATE,
                        Types.TIME, Types.TIMESTAMP},
                new int[]{32, 10, 8, 29}, new int[]{0, 0, 0, 9});

        rs.next();
        assertEquals(0, rs.getInt(1));
        assertTrue(rs.getDate(2, regular) == null && rs.wasNull());
        assertTrue(rs.getTime(3, regular) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, regular) == null && rs.wasNull());

        rs.next();
        assertEquals(1, rs.getInt(1));
        assertTrue(rs.getDate(2, other) == null && rs.wasNull());
        assertTrue(rs.getTime(3, other) == null && rs.wasNull());
        assertTrue(rs.getTimestamp(3, other) == null && rs.wasNull());

        rs.next();
        assertEquals(2, rs.getInt(1));
        assertEquals("2001-02-03", rs.getDate(2, regular).toString());
        assertEquals("04:05:06", rs.getTime(3, regular).toString());
        assertFalse(rs.getTime(3, other).toString().equals("04:05:06"));
        assertEquals("2007-08-09 10:11:12.131415",
                rs.getTimestamp(4, regular).toString());
        assertFalse(rs.getTimestamp(4, other).toString().
                equals("2007-08-09 10:11:12.131415"));

        rs.next();
        assertEquals(3, rs.getInt("ID"));
        assertFalse(rs.getTimestamp("TS", regular).toString().
                equals("2107-08-09 10:11:12.131415"));
        assertEquals("2107-08-09 10:11:12.131415",
                rs.getTimestamp("TS", other).toString());
        assertFalse(rs.getTime("T", regular).toString().equals("14:05:06"));
        assertEquals("14:05:06",
                rs.getTime("T", other).toString());
        // checkFalse(rs.getDate(2, regular).toString(), "2101-02-03");
        // check(rs.getDate("D", other).toString(), "2101-02-03");

        rs.next();
        assertEquals(4, rs.getInt("ID"));
        assertEquals("2107-08-09 10:11:12.131415",
                rs.getTimestamp("TS").toString());
        assertEquals("14:05:06", rs.getTime("T").toString());
        assertEquals("2101-02-03", rs.getDate("D").toString());

        rs.next();
        assertEquals(5, rs.getInt("ID"));
        assertEquals("2107-08-09 10:11:12.131415",
                rs.getTimestamp("TS").toString());
        assertEquals("14:05:06", rs.getTime("T").toString());
        assertEquals("2101-02-03", rs.getDate("D").toString());

        assertFalse(rs.next());
        stmt.execute("DROP TABLE TEST");*/
    }


    @Test
    public void testBooleanString() throws SQLException {
        stmt.executeUpdate("drop table if exists testboolstring");
        stmt.executeUpdate("create table testboolstring(a varchar(30), b boolean)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('1 ', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('0', false)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES(' t', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('f', false)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('True', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('      False   ', false)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('yes', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('  no  ', false)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('y', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('n', false)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('oN', true)");
        stmt.executeUpdate("INSERT INTO testboolstring VALUES('oFf', false)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('OK', null)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('NOT', null)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('not a boolean', null)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('1.0', null)");
        //stmt.executeUpdate("INSERT INTO testboolstring VALUES('0.0', null)");

        testBoolean("testboolstring");
    }

    @Test
    public void testBooleanFloat() throws SQLException {
        stmt.executeUpdate("drop table if exists testboolfloat");
        stmt.executeUpdate("create table testboolfloat(a float4, b boolean)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES('1.0'::real, true)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES('0.0'::real, false)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES(1.000::real, true)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES(0.000::real, false)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES('1.001'::real, null)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES('-1.001'::real, null)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES(123.4::real, null)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES(1.234e2::real, null)");
        stmt.executeUpdate("INSERT INTO testboolfloat VALUES(100.00e-2::real, true)");

        testBoolean("testboolfloat");
    }

    @Test
    public void testBooleanInt() throws SQLException {
        stmt.executeUpdate("drop table if exists testboolint");
        stmt.executeUpdate("create table testboolint(a bigint, b boolean)");
        stmt.executeUpdate("INSERT INTO testboolint VALUES(1, true)");
        stmt.executeUpdate("INSERT INTO testboolint VALUES(0, false)");
        stmt.executeUpdate("INSERT INTO testboolint VALUES(-1, null)");
        stmt.executeUpdate("INSERT INTO testboolint VALUES(9223372036854775807, null)");
        stmt.executeUpdate("INSERT INTO testboolint VALUES(-9223372036854775808, null)");

        testBoolean("testboolint");
    }

    public void testBoolean(String table) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("select a, b from " + table);
        ResultSet rs = pstmt.executeQuery();
        while (rs.next()) {
            rs.getBoolean(2);
            Boolean expected = rs.wasNull() ? null : rs.getBoolean(2); // Hack to get SQL NULL
            if (expected != null) {
                assertEquals(expected, rs.getBoolean(1));
            } else {
                // expected value with null are bad values
                try {
                    rs.getBoolean(1);
                    fail();
                } catch (SQLException e) {
                    //assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
                    System.out.println(e.getSQLState());
                }
            }
        }
        rs.close();
        pstmt.close();
    }


    @Test
    void testgetBooleanJDBCCompliance() throws SQLException {
        // The JDBC specification in Table B-6 "Use of ResultSet getter Methods to Retrieve JDBC Data Types"
        // the getBoolean have this Supported JDBC Type: TINYINT, SMALLINT, INTEGER, BIGINT, REAL, FLOAT,
        // DOUBLE, DECIAML, NUMERIC, BIT, BOOLEAN, CHAR, VARCHAR, LONGVARCHAR

        // There is no TINYINT in PostgreSQL
        testgetBoolean("int2"); // SMALLINT
        testgetBoolean("int4"); // INTEGER
        testgetBoolean("int8"); // BIGINT
        testgetBoolean("float4"); // REAL
        testgetBoolean("float8"); // FLOAT, DOUBLE
        testgetBoolean("numeric"); // DECIMAL, NUMERIC
        testgetBoolean("bpchar"); // CHAR
        testgetBoolean("varchar"); // VARCHAR
        testgetBoolean("text"); // LONGVARCHAR?
    }

    public void testgetBoolean(String dataType) throws SQLException {
        ResultSet rs = stmt.executeQuery("select 1::" + dataType + ", 0::" + dataType + ", 2::" + dataType);
        ResultSetMetaData meta = rs.getMetaData();
        for (int i = 1; i <= meta.getColumnCount(); i++) {
            meta.getColumnLabel(i);
        }
        assertTrue(rs.next());
        assertEquals(true, rs.getBoolean(1));
        assertEquals(false, rs.getBoolean(2));

        try {
            // The JDBC ResultSet JavaDoc states that only 1 and 0 are valid values, so 2 should return error.
            rs.getBoolean(3);
            fail();
        } catch (SQLException e) {
            //assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            System.out.println(e.getSQLState());
            // message can be 2 or 2.0 depending on whether binary or text
            final String message = e.getMessage();
            if (!"Cannot cast to boolean: \"2.0\"".equals(message)) {
                assertEquals("Cannot cast to boolean: \"2\"", message);
            }
        }
        rs.close();
        stmt.close();
    }

    @Test
    public void testgetBadBoolean() throws SQLException {
        testBadBoolean("'2017-03-13 14:25:48.130861'::timestamp", "2017-03-13 14:25:48.130861");
        testBadBoolean("'2017-03-13'::date", "2017-03-13");
        testBadBoolean("'2017-03-13 14:25:48.130861'::time", "14:25:48.130861");
        testBadBoolean("ARRAY[[1,0],[0,1]]", "{{1,0},{0,1}}");
        testBadBoolean("29::bit(4)", "1101");
    }

    public void testBadBoolean(String select, String value) throws SQLException {
        ResultSet rs = stmt.executeQuery("select " + select);
        assertTrue(rs.next());
        try {
            rs.getBoolean(1);
            fail();
        } catch (SQLException e) {
            //binary transfer gets different error code and message
            /*if (org.postgresql.util.PSQLState.DATA_TYPE_MISMATCH.getState().equals(e.getSQLState())) {
                final String message = e.getMessage();
                if (!message.startsWith("Cannot convert the column of type ")) {
                    fail(message);
                }
                if (!message.endsWith(" to requested type boolean.")) {
                    fail(message);
                }
            } else {
                //assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
                assertEquals("Cannot cast to boolean: \"" + value + "\"", e.getMessage());
            }*/
        }
        rs.close();
        stmt.close();
    }

/*
    @Test
    public void testgetShort() throws SQLException {
        ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

        assertTrue(rs.next());
        assertEquals(1, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(-1, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(-2, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(10, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getShort(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getShort(1));

        while (rs.next()) {
            try {
                rs.getShort(1);
                fail("Exception expected.");
            } catch (Exception e) {
            }
        }
        rs.close();
    }

    @Test
    public void testgetInt() throws SQLException {
        ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(-1, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(-2, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(10, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(99999, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(99999, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(-99999, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(-99999, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(Integer.MAX_VALUE, rs.getInt(1));

        assertTrue(rs.next());
        assertEquals(Integer.MIN_VALUE, rs.getInt(1));

        while (rs.next()) {
            try {
                rs.getInt(1);
                fail("Exception expected." + rs.getString(1));
            } catch (Exception e) {
            }
        }
        rs.close();
    }

    @Test
    public void testgetLong() throws SQLException {
        ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(-1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(-2, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(10, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(0, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(99999, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(99999, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(-99999, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(-99999, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals((Integer.MAX_VALUE), rs.getLong(1));

        assertTrue(rs.next());
        assertEquals((Integer.MIN_VALUE), rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(((long) Integer.MAX_VALUE) + 1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(((long) Integer.MIN_VALUE) - 1, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(Long.MAX_VALUE, rs.getLong(1));

        assertTrue(rs.next());
        assertEquals(Long.MIN_VALUE, rs.getLong(1));

        while (rs.next()) {
            try {
                rs.getLong(1);
                fail("Exception expected." + rs.getString(1));
            } catch (Exception e) {
            }
        }
        rs.close();
    }

    @Test
    public void testgetBigDecimal() throws SQLException {
        ResultSet rs = con.createStatement().executeQuery("select * from testnumeric");

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(1.0), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(0.0), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(-1.0), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(1.2), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(-2.5), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.000000000000000000000000000990"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("10.0000000000099"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.10000000000000"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("0.10"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("1.10000000000000"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(99999.2), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(99999), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(-99999.2), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(-99999), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(2147483647), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(BigDecimal.valueOf(-2147483648), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("2147483648"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("-2147483649"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("9223372036854775807"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("-9223372036854775808"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("9223372036854775808"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("-9223372036854775809"), rs.getBigDecimal(1));

        assertTrue(rs.next());
        assertEquals(new BigDecimal("10223372036850000000"), rs.getBigDecimal(1));
    }

    @Test
    public void testGetOutOfBounds() throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT id FROM testrs");
        assertTrue(rs.next());

        try {
            rs.getInt(-9);
        } catch (SQLException sqle) {
        }

        try {
            rs.getInt(1000);
        } catch (SQLException sqle) {
        }
    }
*/

    @Test
    void testClosedResult() throws SQLException {
        Statement stmt =
                conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs = stmt.executeQuery("SELECT id FROM testrs");
        rs.close();

        rs.close(); // Closing twice is allowed.
        try {
            rs.getInt(1);
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.getInt("id");
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.getType();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.wasNull();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.absolute(3);
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.isBeforeFirst();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.setFetchSize(10);
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.getMetaData();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.rowUpdated();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.updateInt(1, 1);
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.moveToInsertRow();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
        try {
            rs.clearWarnings();
            fail("Expected SQLException");
        } catch (SQLException e) {
        }
    }

    /*
     * The JDBC spec says when you have duplicate column names, the first one should be returned.
     */
    @Test
    void testDuplicateColumnNameOrder() throws SQLException {
        ResultSet rs = stmt.executeQuery("SELECT 1 AS a, 2 AS a");
        assertTrue(rs.next());
        assertEquals(1, rs.getInt("a"));
    }


    @Test
    void next() {
    }

    @Test
    void close() {
    }

    @Test
    void wasNull() {
    }

    @Test
    void getString() {
    }

    @Test
    void getBoolean() {
    }

    @Test
    void getByte() {
    }

    @Test
    void getShort() {
    }

    @Test
    void getInt() {
    }

    @Test
    void getLong() {
    }

    @Test
    void getFloat() {
    }

    @Test
    void getDouble() {
    }

    @Test
    void getBigDecimal() {
    }

    @Test
    void getBytes() {
    }

    @Test
    void getDate() {
    }

    @Test
    void getTime() {
    }

    @Test
    void getTimestamp() {
    }

    @Test
    void getAsciiStream() {
    }

    @Test
    void getUnicodeStream() {
    }

    @Test
    void getBinaryStream() {
    }

    @Test
    void testGetString() {
    }

    @Test
    void testGetBoolean() {
    }

    @Test
    void testGetByte() {
    }

    @Test
    void testGetShort() {
    }

    @Test
    void testGetInt() {
    }

    @Test
    void testGetLong() {
    }

    @Test
    void testGetFloat() {
    }

    @Test
    void testGetDouble() {
    }

    @Test
    void testGetBigDecimal() {
    }

    @Test
    void testGetBytes() {
    }

    @Test
    void testGetDate() {
    }

    @Test
    void testGetTime() {
    }

    @Test
    void testGetTimestamp() {
    }

    @Test
    void testGetAsciiStream() {
    }

    @Test
    void testGetUnicodeStream() {
    }

    @Test
    void testGetBinaryStream() {
    }

    @Test
    void getWarnings() {
    }

    @Test
    void clearWarnings() {
    }

    @Test
    void getCursorName() {
    }

    @Test
    void getMetaData() {
    }

    @Test
    void getObject() {
    }

    @Test
    void testGetObject() {
    }

    @Test
    void findColumn() {
    }

    @Test
    void getCharacterStream() {
    }

    @Test
    void testGetCharacterStream() {
    }

    @Test
    void testGetBigDecimal1() {
    }

    @Test
    void testGetBigDecimal2() {
    }

    @Test
    void isBeforeFirst() {
    }

    @Test
    void isAfterLast() {
    }

    @Test
    void isFirst() {
    }

    @Test
    void isLast() {
    }

    @Test
    void beforeFirst() {
    }

    @Test
    void afterLast() {
    }

    @Test
    void first() {
    }

    @Test
    void last() {
    }

    @Test
    void getRow() {
    }

    @Test
    void absolute() {
    }

    @Test
    void relative() {
    }

    @Test
    void previous() {
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
    void getType() {
    }

    @Test
    void getConcurrency() {
    }

    @Test
    void rowUpdated() {
    }

    @Test
    void rowInserted() {
    }

    @Test
    void rowDeleted() {
    }

    @Test
    void updateNull() {
    }

    @Test
    void updateBoolean() {
    }

    @Test
    void updateByte() {
    }

    @Test
    void updateShort() {
    }

    @Test
    void updateInt() {
    }

    @Test
    void updateLong() {
    }

    @Test
    void updateFloat() {
    }

    @Test
    void updateDouble() {
    }

    @Test
    void updateBigDecimal() {
    }

    @Test
    void updateString() {
    }

    @Test
    void updateBytes() {
    }

    @Test
    void updateDate() {
    }

    @Test
    void updateTime() {
    }

    @Test
    void updateTimestamp() {
    }

    @Test
    void updateAsciiStream() {
    }

    @Test
    void updateBinaryStream() {
    }

    @Test
    void updateCharacterStream() {
    }

    @Test
    void updateObject() {
    }

    @Test
    void testUpdateObject() {
    }

    @Test
    void testUpdateNull() {
    }

    @Test
    void testUpdateBoolean() {
    }

    @Test
    void testUpdateByte() {
    }

    @Test
    void testUpdateShort() {
    }

    @Test
    void testUpdateInt() {
    }

    @Test
    void testUpdateLong() {
    }

    @Test
    void testUpdateFloat() {
    }

    @Test
    void testUpdateDouble() {
    }

    @Test
    void testUpdateBigDecimal() {
    }

    @Test
    void testUpdateString() {
    }

    @Test
    void testUpdateBytes() {
    }

    @Test
    void testUpdateDate() {
    }

    @Test
    void testUpdateTime() {
    }

    @Test
    void testUpdateTimestamp() {
    }

    @Test
    void testUpdateAsciiStream() {
    }

    @Test
    void testUpdateBinaryStream() {
    }

    @Test
    void testUpdateCharacterStream() {
    }

    @Test
    void testUpdateObject1() {
    }

    @Test
    void testUpdateObject2() {
    }

    @Test
    void insertRow() {
    }

    @Test
    void updateRow() {
    }

    @Test
    void deleteRow() {
    }

    @Test
    void refreshRow() {
    }

    @Test
    void cancelRowUpdates() {
    }

    @Test
    void moveToInsertRow() {
    }

    @Test
    void moveToCurrentRow() {
    }

    @Test
    void getStatement() {
    }

    @Test
    void testGetObject1() {
    }

    @Test
    void getRef() {
    }

    @Test
    void getBlob() {
    }

    @Test
    void getClob() {
    }

    @Test
    void getArray() {
    }

    @Test
    void testGetObject2() {
    }

    @Test
    void testGetRef() {
    }

    @Test
    void testGetBlob() {
    }

    @Test
    void testGetClob() {
    }

    @Test
    void testGetArray() {
    }

    @Test
    void testGetDate1() {
    }

    @Test
    void testGetDate2() {
    }

    @Test
    void testGetTime1() {
    }

    @Test
    void testGetTime2() {
    }

    @Test
    void testGetTimestamp1() {
    }

    @Test
    void testGetTimestamp2() {
    }

    @Test
    void getURL() {
    }

    @Test
    void testGetURL() {
    }

    @Test
    void updateRef() {
    }

    @Test
    void testUpdateRef() {
    }

    @Test
    void updateBlob() {
    }

    @Test
    void testUpdateBlob() {
    }

    @Test
    void updateClob() {
    }

    @Test
    void testUpdateClob() {
    }

    @Test
    void updateArray() {
    }

    @Test
    void testUpdateArray() {
    }

    @Test
    void getRowId() {
    }

    @Test
    void testGetRowId() {
    }

    @Test
    void updateRowId() {
    }

    @Test
    void testUpdateRowId() {
    }

    @Test
    void getHoldability() {
    }

    @Test
    void isClosed() {
    }

    @Test
    void updateNString() {
    }

    @Test
    void testUpdateNString() {
    }

    @Test
    void updateNClob() {
    }

    @Test
    void testUpdateNClob() {
    }

    @Test
    void getNClob() {
    }

    @Test
    void testGetNClob() {
    }

    @Test
    void getSQLXML() {
    }

    @Test
    void testGetSQLXML() {
    }

    @Test
    void updateSQLXML() {
    }

    @Test
    void testUpdateSQLXML() {
    }

    @Test
    void getNString() {
    }

    @Test
    void testGetNString() {
    }

    @Test
    void getNCharacterStream() {
    }

    @Test
    void testGetNCharacterStream() {
    }

    @Test
    void updateNCharacterStream() {
    }

    @Test
    void testUpdateNCharacterStream() {
    }

    @Test
    void testUpdateAsciiStream1() {
    }

    @Test
    void testUpdateBinaryStream1() {
    }

    @Test
    void testUpdateCharacterStream1() {
    }

    @Test
    void testUpdateAsciiStream2() {
    }

    @Test
    void testUpdateBinaryStream2() {
    }

    @Test
    void testUpdateCharacterStream2() {
    }

    @Test
    void testUpdateBlob1() {
    }

    @Test
    void testUpdateBlob2() {
    }

    @Test
    void testUpdateClob1() {
    }

    @Test
    void testUpdateClob2() {
    }

    @Test
    void testUpdateNClob1() {
    }

    @Test
    void testUpdateNClob2() {
    }

    @Test
    void testUpdateNCharacterStream1() {
    }

    @Test
    void testUpdateNCharacterStream2() {
    }

    @Test
    void testUpdateAsciiStream3() {
    }

    @Test
    void testUpdateBinaryStream3() {
    }

    @Test
    void testUpdateCharacterStream3() {
    }

    @Test
    void testUpdateAsciiStream4() {
    }

    @Test
    void testUpdateBinaryStream4() {
    }

    @Test
    void testUpdateCharacterStream4() {
    }

    @Test
    void testUpdateBlob3() {
    }

    @Test
    void testUpdateBlob4() {
    }

    @Test
    void testUpdateClob3() {
    }

    @Test
    void testUpdateClob4() {
    }

    @Test
    void testUpdateNClob3() {
    }

    @Test
    void testUpdateNClob4() {
    }

    @Test
    void testGetObject3() {
    }

    @Test
    void testGetObject4() {
    }

    @Test
    void unwrap() throws SQLException {
        rs.unwrap(Object.class);
    }

    @Test
    void isWrapperFor() {
    }
}