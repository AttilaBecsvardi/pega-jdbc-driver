package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PegaCallableStatementTest {

    static Properties config;
    static String url;
    static Connection conn;
    static CallableStatement cStmt;

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
        if (cStmt != null && !cStmt.isClosed()) {
            cStmt.close();
        }
        if (conn != null) {
            conn.close();
        }
    }

    @Test
    @Disabled
    void registerOutParameter() {
    }

    @Test
    @Disabled
    void testRegisterOutParameter() {
    }


    @Test
    void testExecute_FunctionCall() throws SQLException {
        cStmt = conn.prepareCall("{ ? = call upper( ? ) }");
        cStmt.registerOutParameter(1, Types.VARCHAR);
        cStmt.setString(2, "lowercase to uppercase");
        cStmt.execute();
        String upperCased = cStmt.getString(1);
        cStmt.close();
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
    void getObject() {
    }

    @Test
    void testGetBigDecimal() {
    }

    @Test
    void testGetObject() {
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
    void testGetDate() {
    }

    @Test
    void testGetTime() {
    }

    @Test
    void testGetTimestamp() {
    }

    @Test
    void testRegisterOutParameter1() {
    }

    @Test
    void testRegisterOutParameter2() {
    }

    @Test
    void testRegisterOutParameter3() {
    }

    @Test
    void testRegisterOutParameter4() {
    }

    @Test
    void getURL() {
    }

    @Test
    void setURL() {
    }

    @Test
    void setNull() {
    }

    @Test
    void setBoolean() {
    }

    @Test
    void setByte() {
    }

    @Test
    void setShort() {
    }

    @Test
    void setInt() {
    }

    @Test
    void setLong() {
    }

    @Test
    void setFloat() {
    }

    @Test
    void setDouble() {
    }

    @Test
    void setBigDecimal() {
    }

    @Test
    void setString() {
    }

    @Test
    void setBytes() {
    }

    @Test
    void setDate() {
    }

    @Test
    void setTime() {
    }

    @Test
    void setTimestamp() {
    }

    @Test
    void setAsciiStream() {
    }

    @Test
    void setBinaryStream() {
    }

    @Test
    void setObject() {
    }

    @Test
    void testSetObject() {
    }

    @Test
    void testSetObject1() {
    }

    @Test
    void setCharacterStream() {
    }

    @Test
    void testSetDate() {
    }

    @Test
    void testSetTime() {
    }

    @Test
    void testSetTimestamp() {
    }

    @Test
    void testSetNull() {
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
    void testGetBytes() {
    }

    @Test
    void testGetDate1() {
    }

    @Test
    void testGetTime1() {
    }

    @Test
    void testGetTimestamp1() {
    }

    @Test
    void testGetObject1() {
    }

    @Test
    void testGetBigDecimal1() {
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
    void testGetDate2() {
    }

    @Test
    void testGetTime2() {
    }

    @Test
    void testGetTimestamp2() {
    }

    @Test
    void testGetURL() {
    }

    @Test
    void getRowId() {
    }

    @Test
    void testGetRowId() {
    }

    @Test
    void setRowId() {
    }

    @Test
    void setNString() {
    }

    @Test
    void setNCharacterStream() {
    }

    @Test
    void setNClob() {
    }

    @Test
    void setClob() {
    }

    @Test
    void setBlob() {
    }

    @Test
    void testSetNClob() {
    }

    @Test
    void getNClob() {
    }

    @Test
    void testGetNClob() {
    }

    @Test
    void setSQLXML() {
    }

    @Test
    void getSQLXML() {
    }

    @Test
    void testGetSQLXML() {
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
    void getCharacterStream() {
    }

    @Test
    void testGetCharacterStream() {
    }

    @Test
    void testSetBlob() {
    }

    @Test
    void testSetClob() {
    }

    @Test
    void testSetAsciiStream() {
    }

    @Test
    void testSetBinaryStream() {
    }

    @Test
    void testSetCharacterStream() {
    }

    @Test
    void testSetAsciiStream1() {
    }

    @Test
    void testSetBinaryStream1() {
    }

    @Test
    void testSetCharacterStream1() {
    }

    @Test
    void testSetNCharacterStream() {
    }

    @Test
    void testSetClob1() {
    }

    @Test
    void testSetBlob1() {
    }

    @Test
    void testSetNClob1() {
    }

    @Test
    void testGetObject3() {
    }

    @Test
    void testGetObject4() {
    }
}