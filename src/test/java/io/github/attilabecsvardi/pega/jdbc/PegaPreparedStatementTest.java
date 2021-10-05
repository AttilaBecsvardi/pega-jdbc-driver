package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PegaPreparedStatementTest {
    static Properties config;
    static String url;
    static Connection conn;
    static PreparedStatement prep;

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
        if (prep != null) {
            prep.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    void testChangeType() throws SQLException {
        PreparedStatement prep = conn.prepareStatement(
                "select (? || ? || ?)");
        prep.setString(1, "a");
        prep.setString(2, "b");
        prep.setString(3, "c");
        prep.executeQuery();
        prep.setInt(1, 1);
        prep.setString(2, "ab");
        prep.setInt(3, 45);
        prep.executeQuery();
    }

    @Test
    void testExecuteQuery() throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        stat.execute("CREATE TABLE TEST(FIELD INT PRIMARY KEY)");
        stat.execute("INSERT INTO TEST VALUES(1)");
        stat.execute("INSERT INTO TEST VALUES(2)");
        prep = conn.prepareStatement("select FIELD FROM "
                + "(select FIELD FROM (SELECT FIELD  FROM TEST "
                + "WHERE FIELD = ?) AS T2 "
                + "WHERE T2.FIELD = ?) AS T3 WHERE T3.FIELD = ?");
        prep.setInt(1, 1);
        prep.setInt(2, 1);
        prep.setInt(3, 1);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        prep.setInt(1, 2);
        prep.setInt(2, 2);
        prep.setInt(3, 2);
        rs = prep.executeQuery();
        rs.next();
        assertEquals(2, rs.getInt(1));
        stat.execute("DROP TABLE TEST");
    }

    @Test
    void testInsertFunction() throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;

        stat.execute("CREATE TABLE TEST(ID INT, H VARCHAR)");
        prep = conn.prepareStatement("INSERT INTO TEST " +
                "VALUES(?, ?)");
        prep.setInt(1, 1);
        prep.setString(2, "One");
        prep.execute();
        prep.setInt(1, 2);
        prep.setString(2, "Two");
        prep.execute();
        rs = stat.executeQuery("SELECT COUNT(DISTINCT H) FROM TEST");
        rs.next();
        assertEquals(2, rs.getInt(1));

        stat.execute("DROP TABLE TEST");
    }

    @Test
    void testDate() throws SQLException {
        PreparedStatement prep = conn.prepareStatement("SELECT ?");
        Timestamp ts = Timestamp.valueOf("2001-02-03 04:05:06");
        prep.setTime(1, new Time(ts.getTime()));
        ResultSet rs = prep.executeQuery();
        rs.next();
        Timestamp ts2 = rs.getTimestamp(1);
        assertEquals(ts.toString(), ts2.toString());
    }

    @Test
    void testLikeIndex() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT PRIMARY KEY, V INT, NAME VARCHAR(255))");
        stat.execute("INSERT INTO TEST VALUES(1, 2, 'Hello')");
        stat.execute("INSERT INTO TEST VALUES(2, 4, 'World')");
        stat.execute("create index idxname on test(name);");
        PreparedStatement prep, prepExe;

        prep = conn.prepareStatement(
                "EXPLAIN SELECT * FROM TEST WHERE NAME LIKE ?");
        //assertEquals(1, prep.getParameterMetaData().getParameterCount());
        prepExe = conn.prepareStatement(
                "SELECT * FROM TEST WHERE NAME LIKE ?");
        prep.setString(1, "%orld");
        prepExe.setString(1, "%orld");
        ResultSet rs = prep.executeQuery();
        rs.next();
        String plan = rs.getString(1);
        assertTrue(plan.contains("Scan"));
        rs = prepExe.executeQuery();
        rs.next();
        assertEquals("World", rs.getString(3));
        assertFalse(rs.next());

        prep.setString(1, "H%");
        prepExe.setString(1, "H%");
        rs = prep.executeQuery();
        rs.next();
        String plan1 = rs.getString(1);
        assertTrue(plan1.contains("Scan"));
        rs = prepExe.executeQuery();
        rs.next();
        assertEquals("Hello", rs.getString(3));
        assertFalse(rs.next());

        stat.execute("DROP TABLE IF EXISTS TEST");
    }

    @Test
    void testSubquery() throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1),(2),(3)");
        PreparedStatement prep = conn.prepareStatement("select x.id, ? from "
                + "(select * from test where id in(?, ?)) x where x.id*2 <>  ?");
        //assertEquals(4, prep.getParameterMetaData().getParameterCount());
        prep.setInt(1, 0);
        prep.setInt(2, 1);
        prep.setInt(3, 2);
        prep.setInt(4, 4);
        ResultSet rs = prep.executeQuery();
        rs.next();
        assertEquals(1, rs.getInt(1));
        assertEquals(0, rs.getInt(2));
        assertFalse(rs.next());
        stat.execute("DROP TABLE TEST");
    }

    @Test
    void testGetMoreResults() throws SQLException {
        Statement stat = conn.createStatement();
        PreparedStatement prep;
        ResultSet rs;
        stat.execute("DROP TABLE IF EXISTS TEST");
        stat.execute("CREATE TABLE TEST(ID INT)");
        stat.execute("INSERT INTO TEST VALUES(1)");

        prep = conn.prepareStatement("SELECT * FROM TEST");
        // just to check if it doesn't throw an exception - it may be null
        //prep.getMetaData();
        assertTrue(prep.execute());
        rs = prep.getResultSet();
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        // supposed to be closed now
        //assertThrows(SQLException.class, rs::next);
        assertEquals(-1, prep.getUpdateCount());

        prep = conn.prepareStatement("UPDATE TEST SET ID = 2");
        assertFalse(prep.execute());
        assertEquals(1, prep.getUpdateCount());
        assertFalse(prep.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
        assertEquals(-1, prep.getUpdateCount());
        // supposed to be closed now
        //assertThrows(SQLException.class, rs::next);
        assertEquals(-1, prep.getUpdateCount());

        prep = conn.prepareStatement("DELETE FROM TEST");
        prep.executeUpdate();
        assertFalse(prep.getMoreResults());
        assertEquals(-1, prep.getUpdateCount());
        stat.execute("DROP TABLE TEST");
    }

    @Test
    void testExecuteStringOnPreparedStatement() throws Exception {
        PreparedStatement pstmt = conn.prepareStatement("SELECT 1");

        try {
            pstmt.executeQuery("SELECT 2");
            fail("Expected an exception when executing a new SQL query on a prepared statement");
        } catch (SQLException e) {
        }

        try {
            pstmt.executeUpdate("UPDATE streamtable SET bin=bin");
            fail("Expected an exception when executing a new SQL update on a prepared statement");
        } catch (SQLException e) {
        }

        try {
            pstmt.execute("UPDATE streamtable SET bin=bin");
            fail("Expected an exception when executing a new SQL statement on a prepared statement");
        } catch (SQLException e) {
        }
    }

    /*
        @Test
        void testBinds() throws SQLException {
            // braces around (42) are required to puzzle the parser
            xreatetable
            String query = "INSERT INTO inttable(a) VALUES (?);SELECT (42)";
            PreparedStatement ps = conn.prepareStatement(query);
            ps.setInt(1, 100500);
            ps.execute();
            ResultSet rs = ps.getResultSet();
            assertNull(rs,"insert produces no results ==> getResultSet should be null");
            assertTrue(ps.getMoreResults());
            rs = ps.getResultSet();
            assertNotNull(rs, "select produces results ==> getResultSet should be not null");
            assertTrue(rs.next(), "select produces 1 row ==> rs.next should be true");
            assertEquals( 42, rs.getInt(1));

            rs.close();
            ps.close();
        }

        @Test
        public void testSetNull() throws SQLException {
            // valid: fully qualified type to setNull()
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO texttable (te) VALUES (?)");
            pstmt.setNull(1, Types.VARCHAR);
            pstmt.executeUpdate();

            // valid: fully qualified type to setObject()
            pstmt.setObject(1, null, Types.VARCHAR);
            pstmt.executeUpdate();

            // valid: setObject() with partial type info and a typed "null object instance"
            org.postgresql.util.PGobject dummy = new org.postgresql.util.PGobject();
            dummy.setType("text");
            dummy.setValue(null);
            pstmt.setObject(1, dummy, Types.OTHER);
            pstmt.executeUpdate();

            // setObject() with no type info
            pstmt.setObject(1, null);
            pstmt.executeUpdate();

            // setObject() with insufficient type info
            pstmt.setObject(1, null, Types.OTHER);
            pstmt.executeUpdate();

            // setNull() with insufficient type info
            pstmt.setNull(1, Types.OTHER);
            pstmt.executeUpdate();

            pstmt.close();

            assumeMinimumServerVersion(ServerVersion.v8_3);
            pstmt = con.prepareStatement("select 'ok' where ?=? or (? is null) ");
            pstmt.setObject(1, UUID.randomUUID(), Types.OTHER);
            pstmt.setNull(2, Types.OTHER, "uuid");
            pstmt.setNull(3, Types.OTHER, "uuid");
            ResultSet rs = pstmt.executeQuery();

            assertTrue(rs.next());
            assertEquals("ok",rs.getObject(1));

            rs.close();
            pstmt.close();

        }

        @Test
        public void testSingleQuotes() throws SQLException {
            String[] testStrings = new String[]{
                    "bare ? question mark",
                    "quoted \\' single quote",
                    "doubled '' single quote",
                    "octal \\060 constant",
                    "escaped \\? question mark",
                    "double \\\\ backslash",
                    "double \" quote",};

            String[] testStringsStdConf = new String[]{
                    "bare ? question mark",
                    "quoted '' single quote",
                    "doubled '' single quote",
                    "octal 0 constant",
                    "escaped ? question mark",
                    "double \\ backslash",
                    "double \" quote",};

            String[] expected = new String[]{
                    "bare ? question mark",
                    "quoted ' single quote",
                    "doubled ' single quote",
                    "octal 0 constant",
                    "escaped ? question mark",
                    "double \\ backslash",
                    "double \" quote",};

            boolean oldStdStrings = TestUtil.getStandardConformingStrings(con);
            Statement stmt = con.createStatement();

            // Test with standard_conforming_strings turned off.
            stmt.execute("SET standard_conforming_strings TO off");
            for (int i = 0; i < testStrings.length; ++i) {
                PreparedStatement pstmt = con.prepareStatement("SELECT '" + testStrings[i] + "'");
                ResultSet rs = pstmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(expected[i], rs.getString(1));
                rs.close();
                pstmt.close();
            }

            // Test with standard_conforming_strings turned off...
            // ... using the escape string syntax (E'').
            stmt.execute("SET standard_conforming_strings TO on");
            for (int i = 0; i < testStrings.length; ++i) {
                PreparedStatement pstmt = con.prepareStatement("SELECT E'" + testStrings[i] + "'");
                ResultSet rs = pstmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(expected[i], rs.getString(1));
                rs.close();
                pstmt.close();
            }
            // ... using standard conforming input strings.
            for (int i = 0; i < testStrings.length; ++i) {
                PreparedStatement pstmt = con.prepareStatement("SELECT '" + testStringsStdConf[i] + "'");
                ResultSet rs = pstmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(expected[i], rs.getString(1));
                rs.close();
                pstmt.close();
            }

            stmt.execute("SET standard_conforming_strings TO " + (oldStdStrings ? "on" : "off"));
            stmt.close();
        }

        @Test
        public void testDoubleQuotes() throws SQLException {
            String[] testStrings = new String[]{
                    "bare ? question mark",
                    "single ' quote",
                    "doubled '' single quote",
                    "doubled \"\" double quote",
                    "no backslash interpretation here: \\",
            };

            for (String testString : testStrings) {
                PreparedStatement pstmt =
                        con.prepareStatement("CREATE TABLE \"" + testString + "\" (i integer)");
                pstmt.executeUpdate();
                pstmt.close();

                pstmt = con.prepareStatement("DROP TABLE \"" + testString + "\"");
                pstmt.executeUpdate();
                pstmt.close();
            }
        }

        @Test
        public void testDollarQuotes() throws SQLException {
            // dollar-quotes are supported in the backend since version 8.0
            PreparedStatement st;
            ResultSet rs;

            st = con.prepareStatement("SELECT $$;$$ WHERE $x$?$x$=$_0$?$_0$ AND $$?$$=?");
            st.setString(1, "?");
            rs = st.executeQuery();
            assertTrue(rs.next());
            assertEquals(";", rs.getString(1));
            assertFalse(rs.next());
            st.close();

            st = con.prepareStatement(
                    "SELECT $__$;$__$ WHERE ''''=$q_1$'$q_1$ AND ';'=?;"
                            + "SELECT $x$$a$;$x $a$$x$ WHERE $$;$$=? OR ''=$c$c$;$c$;"
                            + "SELECT ?");
            st.setString(1, ";");
            st.setString(2, ";");
            st.setString(3, "$a$ $a$");

            assertTrue(st.execute());
            rs = st.getResultSet();
            assertTrue(rs.next());
            assertEquals(";", rs.getString(1));
            assertFalse(rs.next());

            assertTrue(st.getMoreResults());
            rs = st.getResultSet();
            assertTrue(rs.next());
            assertEquals("$a$;$x $a$", rs.getString(1));
            assertFalse(rs.next());

            assertTrue(st.getMoreResults());
            rs = st.getResultSet();
            assertTrue(rs.next());
            assertEquals("$a$ $a$", rs.getString(1));
            assertFalse(rs.next());
            st.close();
        }

        @Test
        public void testDollarQuotesAndIdentifiers() throws SQLException {
            // dollar-quotes are supported in the backend since version 8.0
            PreparedStatement st;

            con.createStatement().execute("CREATE TEMP TABLE a$b$c(a varchar, b varchar)");
            st = con.prepareStatement("INSERT INTO a$b$c (a, b) VALUES (?, ?)");
            st.setString(1, "a");
            st.setString(2, "b");
            st.executeUpdate();
            st.close();

            con.createStatement().execute("CREATE TEMP TABLE e$f$g(h varchar, e$f$g varchar) ");
            st = con.prepareStatement("UPDATE e$f$g SET h = ? || e$f$g");
            st.setString(1, "a");
            st.executeUpdate();
            st.close();
        }

        @Test*/
    public void testComments() throws SQLException {
        PreparedStatement st;
        // ResultSet rs;

        st = conn.prepareStatement("SELECT /*?*/ /*/*/*/**/*/*/*/1;SELECT ?;--SELECT ?");
     /*  st.setString(1, "a");
        assertTrue(st.execute());
        assertTrue(st.getMoreResults());
        assertFalse(st.getMoreResults());
        st.close();
*/

        st = conn.prepareStatement("SELECT /**/'?'/*/**/*/ WHERE '?'=/*/*/*?*/*/*/--?\n?");
    }/*     st.setString(1, "?");
        rs = st.executeQuery();
        assertTrue(rs.next());
        assertEquals("?", rs.getString(1));
        assertFalse(rs.next());
        st.close();
    }

    @Test
    public void testNumeric() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE TEMP TABLE numeric_tab (max_numeric_positive numeric, min_numeric_positive numeric, max_numeric_negative numeric, min_numeric_negative numeric, null_value numeric)");
        pstmt.executeUpdate();
        pstmt.close();

        char[] wholeDigits = new char[NUMERIC_MAX_DISPLAY_SCALE];
        for (int i = 0; i < NUMERIC_MAX_DISPLAY_SCALE; i++) {
            wholeDigits[i] = '9';
        }

        char[] fractionDigits = new char[NUMERIC_MAX_PRECISION];
        for (int i = 0; i < NUMERIC_MAX_PRECISION; i++) {
            fractionDigits[i] = '9';
        }

        String maxValueString = new String(wholeDigits);
        String minValueString = new String(fractionDigits);
        BigDecimal[] values = new BigDecimal[4];
        values[0] = new BigDecimal(maxValueString);
        values[1] = new BigDecimal("-" + maxValueString);
        values[2] = new BigDecimal(minValueString);
        values[3] = new BigDecimal("-" + minValueString);

        pstmt = con.prepareStatement("insert into numeric_tab values (?,?,?,?,?)");
        for (int i = 1; i < 5 ; i++) {
            pstmt.setBigDecimal(i, values[i - 1]);
        }

        pstmt.setNull(5, Types.NUMERIC);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from numeric_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        for (int i = 1; i < 5 ; i++) {
            assertTrue(rs.getBigDecimal(i).compareTo(values[i - 1]) == 0);
        }
        rs.getDouble(5);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testDouble() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE TEMP TABLE double_tab (max_double float, min_double float, null_value float)");
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("insert into double_tab values (?,?,?)");
        pstmt.setDouble(1, 1.0E125);
        pstmt.setDouble(2, 1.0E-130);
        pstmt.setNull(3, Types.DOUBLE);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from double_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        double d = rs.getDouble(1);
        assertTrue(rs.getDouble(1) == 1.0E125);
        assertTrue(rs.getDouble(2) == 1.0E-130);
        rs.getDouble(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testFloat() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE TEMP TABLE float_tab (max_float real, min_float real, null_value real)");
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("insert into float_tab values (?,?,?)");
        pstmt.setFloat(1, (float) 1.0E37);
        pstmt.setFloat(2, (float) 1.0E-37);
        pstmt.setNull(3, Types.FLOAT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from float_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());
        float f = rs.getFloat(1);
        assertTrue("expected 1.0E37,received " + rs.getFloat(1), rs.getFloat(1) == (float) 1.0E37);
        assertTrue("expected 1.0E-37,received " + rs.getFloat(2), rs.getFloat(2) == (float) 1.0E-37);
        rs.getDouble(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testNaNLiteralsSimpleStatement() throws SQLException {
        Statement stmt = con.createStatement();
        ResultSet rs = stmt.executeQuery("select 'NaN'::numeric, 'NaN'::real, 'NaN'::double precision");
        checkNaNLiterals(stmt, rs);
    }

    @Test
    public void testNaNLiteralsPreparedStatement() throws SQLException {
        PreparedStatement stmt = con.prepareStatement("select 'NaN'::numeric, 'NaN'::real, 'NaN'::double precision");
        checkNaNLiterals(stmt, stmt.executeQuery());
    }

    private void checkNaNLiterals(Statement stmt, ResultSet rs) throws SQLException {
        rs.next();
        assertTrue("Double.isNaN((Double) rs.getObject", Double.isNaN((Double) rs.getObject(3)));
        assertTrue("Double.isNaN(rs.getDouble", Double.isNaN(rs.getDouble(3)));
        assertTrue("Float.isNaN((Float) rs.getObject", Float.isNaN((Float) rs.getObject(2)));
        assertTrue("Float.isNaN(rs.getFloat", Float.isNaN(rs.getFloat(2)));
        assertTrue("Double.isNaN((Double) rs.getObject", Double.isNaN((Double) rs.getObject(1)));
        assertTrue("Double.isNaN(rs.getDouble", Double.isNaN(rs.getDouble(1)));
        rs.close();
        stmt.close();
    }

    @Test
    public void testNaNSetDoubleFloat() throws SQLException {
        PreparedStatement ps = con.prepareStatement("select ?, ?");
        ps.setFloat(1, Float.NaN);
        ps.setDouble(2, Double.NaN);

        checkNaNParams(ps);
    }


    private void checkNaNParams(PreparedStatement ps) throws SQLException {
        ResultSet rs = ps.executeQuery();
        rs.next();

        assertTrue("Float.isNaN((Float) rs.getObject", Float.isNaN((Float) rs.getObject(1)));
        assertTrue("Float.isNaN(rs.getFloat", Float.isNaN(rs.getFloat(1)));
        assertTrue("Double.isNaN(rs.getDouble", Double.isNaN(rs.getDouble(2)));
        assertTrue("Double.isNaN(rs.getDouble", Double.isNaN(rs.getDouble(2)));

        TestUtil.closeQuietly(rs);
        TestUtil.closeQuietly(ps);
    }

    @Test
    public void testBoolean() throws SQLException {
        testBoolean(0);
        testBoolean(1);
        testBoolean(5);
        testBoolean(-1);
    }

    public void testBoolean(int prepareThreshold) throws SQLException {
        PreparedStatement pstmt = con.prepareStatement("insert into bool_tab values (?,?,?,?,?,?,?,?)");
        ((org.postgresql.PGStatement) pstmt).setPrepareThreshold(prepareThreshold);

        // Test TRUE values
        pstmt.setBoolean(1, true);
        pstmt.setObject(1, Boolean.TRUE);
        pstmt.setNull(2, Types.BIT);
        pstmt.setObject(3, 't', Types.BIT);
        pstmt.setObject(3, 'T', Types.BIT);
        pstmt.setObject(3, "t", Types.BIT);
        pstmt.setObject(4, "true", Types.BIT);
        pstmt.setObject(5, 'y', Types.BIT);
        pstmt.setObject(5, 'Y', Types.BIT);
        pstmt.setObject(5, "Y", Types.BIT);
        pstmt.setObject(6, "YES", Types.BIT);
        pstmt.setObject(7, "On", Types.BIT);
        pstmt.setObject(8, '1', Types.BIT);
        pstmt.setObject(8, "1", Types.BIT);
        assertEquals("one row inserted, true values", 1, pstmt.executeUpdate());
        // Test FALSE values
        pstmt.setBoolean(1, false);
        pstmt.setObject(1, Boolean.FALSE);
        pstmt.setNull(2, Types.BOOLEAN);
        pstmt.setObject(3, 'f', Types.BOOLEAN);
        pstmt.setObject(3, 'F', Types.BOOLEAN);
        pstmt.setObject(3, "F", Types.BOOLEAN);
        pstmt.setObject(4, "false", Types.BOOLEAN);
        pstmt.setObject(5, 'n', Types.BOOLEAN);
        pstmt.setObject(5, 'N', Types.BOOLEAN);
        pstmt.setObject(5, "N", Types.BOOLEAN);
        pstmt.setObject(6, "NO", Types.BOOLEAN);
        pstmt.setObject(7, "Off", Types.BOOLEAN);
        pstmt.setObject(8, "0", Types.BOOLEAN);
        pstmt.setObject(8, '0', Types.BOOLEAN);
        assertEquals("one row inserted, false values", 1, pstmt.executeUpdate());
        // Test weird values
        pstmt.setObject(1, (byte) 0, Types.BOOLEAN);
        pstmt.setObject(2, BigDecimal.ONE, Types.BOOLEAN);
        pstmt.setObject(3, 0L, Types.BOOLEAN);
        pstmt.setObject(4, 0x1, Types.BOOLEAN);
        pstmt.setObject(5, new Float(0), Types.BOOLEAN);
        pstmt.setObject(5, 1.0d, Types.BOOLEAN);
        pstmt.setObject(5, 0.0f, Types.BOOLEAN);
        pstmt.setObject(6, Integer.valueOf("1"), Types.BOOLEAN);
        pstmt.setObject(7, new java.math.BigInteger("0"), Types.BOOLEAN);
        pstmt.clearParameters();
        pstmt.close();

        pstmt = con.prepareStatement("select * from bool_tab");
        ((org.postgresql.PGStatement) pstmt).setPrepareThreshold(prepareThreshold);
        ResultSet rs = pstmt.executeQuery();

        assertTrue(rs.next());
        assertTrue("expected true, received " + rs.getBoolean(1), rs.getBoolean(1));
        rs.getFloat(2);
        assertTrue(rs.wasNull());
        assertTrue("expected true, received " + rs.getBoolean(3), rs.getBoolean(3));
        assertTrue("expected true, received " + rs.getBoolean(4), rs.getBoolean(4));
        assertTrue("expected true, received " + rs.getBoolean(5), rs.getBoolean(5));
        assertTrue("expected true, received " + rs.getBoolean(6), rs.getBoolean(6));
        assertTrue("expected true, received " + rs.getBoolean(7), rs.getBoolean(7));
        assertTrue("expected true, received " + rs.getBoolean(8), rs.getBoolean(8));

        assertTrue(rs.next());
        assertFalse("expected false, received " + rs.getBoolean(1), rs.getBoolean(1));
        rs.getBoolean(2);
        assertTrue(rs.wasNull());
        assertFalse("expected false, received " + rs.getBoolean(3), rs.getBoolean(3));
        assertFalse("expected false, received " + rs.getBoolean(4), rs.getBoolean(4));
        assertFalse("expected false, received " + rs.getBoolean(5), rs.getBoolean(5));
        assertFalse("expected false, received " + rs.getBoolean(6), rs.getBoolean(6));
        assertFalse("expected false, received " + rs.getBoolean(7), rs.getBoolean(7));
        assertFalse("expected false, received " + rs.getBoolean(8), rs.getBoolean(8));

        rs.close();
        pstmt.close();

        pstmt = con.prepareStatement("TRUNCATE TABLE bool_tab");
        pstmt.executeUpdate();
        pstmt.close();
    }

    @Test
    public void testBadBoolean() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement("INSERT INTO bad_bool VALUES (?)");
        try {
            pstmt.setObject(1, "this is not boolean", Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"this is not boolean\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, 'X', Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"X\"", e.getMessage());
        }
        try {
            java.io.File obj = new java.io.File("");
            pstmt.setObject(1, obj, Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean", e.getMessage());
        }
        try {
            pstmt.setObject(1, "1.0", Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"1.0\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, "-1", Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"-1\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, "ok", Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"ok\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, 0.99f, Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"0.99\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, -0.01d, Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"-0.01\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, new java.sql.Date(0), Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean", e.getMessage());
        }
        try {
            pstmt.setObject(1, new java.math.BigInteger("1000"), Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"1000\"", e.getMessage());
        }
        try {
            pstmt.setObject(1, Math.PI, Types.BOOLEAN);
            fail();
        } catch (SQLException e) {
            assertEquals(org.postgresql.util.PSQLState.CANNOT_COERCE.getState(), e.getSQLState());
            assertEquals("Cannot cast to boolean: \"3.141592653589793\"", e.getMessage());
        }
        pstmt.close();
    }

    @Test
    public void testSetFloatInteger() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE float_tab (max_val float8, min_val float, null_val float8)");
        pstmt.executeUpdate();
        pstmt.close();

        Integer maxInteger = new Integer(2147483647);
        Integer minInteger = new Integer(-2147483648);

        Double maxFloat = new Double(2147483647);
        Double minFloat = new Double(-2147483648);

        pstmt = con.prepareStatement("insert into float_tab values (?,?,?)");
        pstmt.setObject(1, maxInteger, Types.FLOAT);
        pstmt.setObject(2, minInteger, Types.FLOAT);
        pstmt.setNull(3, Types.FLOAT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from float_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + maxFloat + " ,received " + rs.getObject(1),
                rs.getObject(1).equals(maxFloat));
        assertTrue("expected " + minFloat + " ,received " + rs.getObject(2),
                rs.getObject(2).equals(minFloat));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetFloatString() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE float_tab (max_val float8, min_val float8, null_val float8)");
        pstmt.executeUpdate();
        pstmt.close();

        String maxStringFloat = "1.0E37";
        String minStringFloat = "1.0E-37";
        Double maxFloat = new Double(1.0E37);
        Double minFloat = new Double(1.0E-37);

        pstmt = con.prepareStatement("insert into float_tab values (?,?,?)");
        pstmt.setObject(1, maxStringFloat, Types.FLOAT);
        pstmt.setObject(2, minStringFloat, Types.FLOAT);
        pstmt.setNull(3, Types.FLOAT);
        pstmt.executeUpdate();
        pstmt.setObject(1, "1.0", Types.FLOAT);
        pstmt.setObject(2, "0.0", Types.FLOAT);
        pstmt.setNull(3, Types.FLOAT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from float_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue(((Double) rs.getObject(1)).equals(maxFloat));
        assertTrue(((Double) rs.getObject(2)).equals(minFloat));
        assertTrue(rs.getDouble(1) == maxFloat);
        assertTrue(rs.getDouble(2) == minFloat);
        rs.getFloat(3);
        assertTrue(rs.wasNull());

        assertTrue(rs.next());
        assertTrue("expected true, received " + rs.getBoolean(1), rs.getBoolean(1));
        assertFalse("expected false,received " + rs.getBoolean(2), rs.getBoolean(2));

        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetFloatBigDecimal() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE float_tab (max_val float8, min_val float8, null_val float8)");
        pstmt.executeUpdate();
        pstmt.close();

        BigDecimal maxBigDecimalFloat = new BigDecimal("1.0E37");
        BigDecimal minBigDecimalFloat = new BigDecimal("1.0E-37");
        Double maxFloat = new Double(1.0E37);
        Double minFloat = new Double(1.0E-37);

        pstmt = con.prepareStatement("insert into float_tab values (?,?,?)");
        pstmt.setObject(1, maxBigDecimalFloat, Types.FLOAT);
        pstmt.setObject(2, minBigDecimalFloat, Types.FLOAT);
        pstmt.setNull(3, Types.FLOAT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from float_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + maxFloat + " ,received " + rs.getObject(1),
                ((Double) rs.getObject(1)).equals(maxFloat));
        assertTrue("expected " + minFloat + " ,received " + rs.getObject(2),
                ((Double) rs.getObject(2)).equals(minFloat));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetTinyIntFloat() throws SQLException {
        PreparedStatement pstmt = con
                .prepareStatement("CREATE temp TABLE tiny_int (max_val int4, min_val int4, null_val int4)");
        pstmt.executeUpdate();
        pstmt.close();

        Integer maxInt = new Integer(127);
        Integer minInt = new Integer(-127);
        Float maxIntFloat = new Float(127);
        Float minIntFloat = new Float(-127);

        pstmt = con.prepareStatement("insert into tiny_int values (?,?,?)");
        pstmt.setObject(1, maxIntFloat, Types.TINYINT);
        pstmt.setObject(2, minIntFloat, Types.TINYINT);
        pstmt.setNull(3, Types.TINYINT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from tiny_int");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertEquals("maxInt as rs.getObject", maxInt, rs.getObject(1));
        assertEquals("minInt as rs.getObject", minInt, rs.getObject(2));
        rs.getObject(3);
        assertTrue("rs.wasNull after rs.getObject", rs.wasNull());
        assertEquals("maxInt as rs.getInt", maxInt, (Integer) rs.getInt(1));
        assertEquals("minInt as rs.getInt", minInt, (Integer) rs.getInt(2));
        rs.getInt(3);
        assertTrue("rs.wasNull after rs.getInt", rs.wasNull());
        assertEquals("maxInt as rs.getLong", Long.valueOf(maxInt), (Long) rs.getLong(1));
        assertEquals("minInt as rs.getLong", Long.valueOf(minInt), (Long) rs.getLong(2));
        rs.getLong(3);
        assertTrue("rs.wasNull after rs.getLong", rs.wasNull());
        assertEquals("maxInt as rs.getBigDecimal", BigDecimal.valueOf(maxInt), rs.getBigDecimal(1));
        assertEquals("minInt as rs.getBigDecimal", BigDecimal.valueOf(minInt), rs.getBigDecimal(2));
        assertNull("rs.getBigDecimal", rs.getBigDecimal(3));
        assertTrue("rs.getBigDecimal after rs.getLong", rs.wasNull());
        assertEquals("maxInt as rs.getBigDecimal(scale=0)", BigDecimal.valueOf(maxInt),
                rs.getBigDecimal(1, 0));
        assertEquals("minInt as rs.getBigDecimal(scale=0)", BigDecimal.valueOf(minInt),
                rs.getBigDecimal(2, 0));
        assertNull("rs.getBigDecimal(scale=0)", rs.getBigDecimal(3, 0));
        assertTrue("rs.getBigDecimal after rs.getLong", rs.wasNull());
        assertEquals("maxInt as rs.getBigDecimal(scale=1)",
                BigDecimal.valueOf(maxInt).setScale(1, BigDecimal.ROUND_HALF_EVEN), rs.getBigDecimal(1, 1));
        assertEquals("minInt as rs.getBigDecimal(scale=1)",
                BigDecimal.valueOf(minInt).setScale(1, BigDecimal.ROUND_HALF_EVEN), rs.getBigDecimal(2, 1));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetSmallIntFloat() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE small_int (max_val int4, min_val int4, null_val int4)");
        pstmt.executeUpdate();
        pstmt.close();

        Integer maxInt = new Integer(32767);
        Integer minInt = new Integer(-32768);
        Float maxIntFloat = new Float(32767);
        Float minIntFloat = new Float(-32768);

        pstmt = con.prepareStatement("insert into small_int values (?,?,?)");
        pstmt.setObject(1, maxIntFloat, Types.SMALLINT);
        pstmt.setObject(2, minIntFloat, Types.SMALLINT);
        pstmt.setNull(3, Types.TINYINT);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from small_int");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + maxInt + " ,received " + rs.getObject(1),
                rs.getObject(1).equals(maxInt));
        assertTrue("expected " + minInt + " ,received " + rs.getObject(2),
                rs.getObject(2).equals(minInt));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();
    }

    @Test
    public void testSetIntFloat() throws SQLException {
        PreparedStatement pstmt = con
                .prepareStatement("CREATE temp TABLE int_TAB (max_val int4, min_val int4, null_val int4)");
        pstmt.executeUpdate();
        pstmt.close();

        Integer maxInt = new Integer(1000);
        Integer minInt = new Integer(-1000);
        Float maxIntFloat = new Float(1000);
        Float minIntFloat = new Float(-1000);

        pstmt = con.prepareStatement("insert into int_tab values (?,?,?)");
        pstmt.setObject(1, maxIntFloat, Types.INTEGER);
        pstmt.setObject(2, minIntFloat, Types.INTEGER);
        pstmt.setNull(3, Types.INTEGER);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from int_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + maxInt + " ,received " + rs.getObject(1),
                ((Integer) rs.getObject(1)).equals(maxInt));
        assertTrue("expected " + minInt + " ,received " + rs.getObject(2),
                ((Integer) rs.getObject(2)).equals(minInt));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetBooleanDouble() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE double_tab (max_val float, min_val float, null_val float)");
        pstmt.executeUpdate();
        pstmt.close();

        Double dBooleanTrue = new Double(1);
        Double dBooleanFalse = new Double(0);

        pstmt = con.prepareStatement("insert into double_tab values (?,?,?)");
        pstmt.setObject(1, Boolean.TRUE, Types.DOUBLE);
        pstmt.setObject(2, Boolean.FALSE, Types.DOUBLE);
        pstmt.setNull(3, Types.DOUBLE);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from double_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1),
                rs.getObject(1).equals(dBooleanTrue));
        assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2),
                rs.getObject(2).equals(dBooleanFalse));
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetBooleanNumeric() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE numeric_tab (max_val numeric(30,15), min_val numeric(30,15), null_val numeric(30,15))");
        pstmt.executeUpdate();
        pstmt.close();

        BigDecimal dBooleanTrue = new BigDecimal(1);
        BigDecimal dBooleanFalse = new BigDecimal(0);

        pstmt = con.prepareStatement("insert into numeric_tab values (?,?,?)");
        pstmt.setObject(1, Boolean.TRUE, Types.NUMERIC, 2);
        pstmt.setObject(2, Boolean.FALSE, Types.NUMERIC, 2);
        pstmt.setNull(3, Types.DOUBLE);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from numeric_tab");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1),
                ((BigDecimal) rs.getObject(1)).compareTo(dBooleanTrue) == 0);
        assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2),
                ((BigDecimal) rs.getObject(2)).compareTo(dBooleanFalse) == 0);
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }

    @Test
    public void testSetBooleanDecimal() throws SQLException {
        PreparedStatement pstmt = con.prepareStatement(
                "CREATE temp TABLE DECIMAL_TAB (max_val numeric(30,15), min_val numeric(30,15), null_val numeric(30,15))");
        pstmt.executeUpdate();
        pstmt.close();

        BigDecimal dBooleanTrue = new BigDecimal(1);
        BigDecimal dBooleanFalse = new BigDecimal(0);

        pstmt = con.prepareStatement("insert into DECIMAL_TAB values (?,?,?)");
        pstmt.setObject(1, Boolean.TRUE, Types.DECIMAL, 2);
        pstmt.setObject(2, Boolean.FALSE, Types.DECIMAL, 2);
        pstmt.setNull(3, Types.DOUBLE);
        pstmt.executeUpdate();
        pstmt.close();

        pstmt = con.prepareStatement("select * from DECIMAL_TAB");
        ResultSet rs = pstmt.executeQuery();
        assertTrue(rs.next());

        assertTrue("expected " + dBooleanTrue + " ,received " + rs.getObject(1),
                ((BigDecimal) rs.getObject(1)).compareTo(dBooleanTrue) == 0);
        assertTrue("expected " + dBooleanFalse + " ,received " + rs.getObject(2),
                ((BigDecimal) rs.getObject(2)).compareTo(dBooleanFalse) == 0);
        rs.getFloat(3);
        assertTrue(rs.wasNull());
        rs.close();
        pstmt.close();

    }
*/
}