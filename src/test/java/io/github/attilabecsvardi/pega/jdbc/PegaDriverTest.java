package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class PegaDriverTest {
    static Properties config;
    static String url;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        config = new Properties();
        try (InputStream is = new FileInputStream("src/test/resources/unit_test.properties")) {
            config.load(is);
            url = config.getProperty("url");
        } catch (Exception e) {
            throw e;
        }
    }

    @Test
    void connect() {
    }

    @Test
        //@Timeout(10)
    void testConnect_ValidInput() throws Exception {

        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        Connection conn = DriverManager.getConnection(url, config);

        assertNotNull(conn);
        assertTrue(conn instanceof PegaConnection);

        conn.close();
    }

    @Test
    void testConnect_InvalidURL() throws Exception {

        String invalidURL = "http://localhost:8080/prweb/PRRestService/JDBCAPI/v1/PegaJDBC/";
        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");

        SQLException e = assertThrows(SQLException.class, () -> {
            Connection conn = DriverManager.getConnection(invalidURL, config);

            if (conn != null) {
                conn.close();
            }
        });

        assertTrue(e.getMessage().startsWith("No suitable driver found for"));
    }

    @Test
    void testConnect_InvalidCredentials() throws Exception {

        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");

        SQLException e = assertThrows(SQLException.class, () -> {
            Properties invalidConfig = new Properties();
            invalidConfig.setProperty("user", "invaliduser");
            invalidConfig.setProperty("password", "invalidpassword");
            invalidConfig.setProperty("dbName", config.getProperty("dbName"));
            Connection conn = DriverManager.getConnection(url, invalidConfig);

            if (conn != null) {
                conn.close();
            }
        });

        assertEquals("Authentication failed", e.getMessage());
    }

    @Test
    void testConnect_MissingDBName() throws Exception {

        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");

        SQLException e = assertThrows(SQLException.class, () -> {
            Properties invalidConfig = new Properties();
            invalidConfig.setProperty("user", config.getProperty("user"));
            invalidConfig.setProperty("password", config.getProperty("password"));
            // dbName is not set
            Connection conn = DriverManager.getConnection(url, invalidConfig);

            if (conn != null) {
                conn.close();
            }
        });

        assertEquals("dbName property is missing", e.getMessage());
    }

    @Test
    void acceptsURL() {
    }

    @Test
    void getPropertyInfo() {
    }

    @Test
    void getMajorVersion() {
    }

    @Test
    void getMinorVersion() {
    }

    @Test
    void jdbcCompliant() {
    }

    @Test
    void getParentLogger() {
    }
}