package io.github.attilabecsvardi.pega.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.*;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
    void testDriverObject() throws Exception {
        PegaDriver instance = PegaDriver.load();
        assertTrue(DriverManager.getDriver("jdbc:pega:~/test") == instance);
        PegaDriver.unload();
        assertThrows(SQLException.class, () -> DriverManager.getDriver("jdbc:pega:~/test"));
        PegaDriver.load();
        assertTrue(DriverManager.getDriver("jdbc:pega:~/test") == instance);
    }

    @Test
    @Disabled("already tested")
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
    void testReuseConnection() throws Exception {
        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        Connection conn = DriverManager.getConnection(url, config);
        assertNotNull(conn);
        conn.close();

        conn = DriverManager.getConnection(url, config);
        assertNotNull(conn);
        String ret = conn.getSchema();
        assertNotNull(ret);
        conn.close();
    }

    @Test
    public void testRegistration() throws Exception {
        // Driver is initially registered because it is automatically done when class is loaded
        assertTrue(io.github.attilabecsvardi.pega.jdbc.PegaDriver.isRegistered());

        ArrayList<Driver> drivers = Collections.list(DriverManager.getDrivers());
        searchInstanceOf:
        {

            for (java.sql.Driver driver : drivers) {
                if (driver instanceof io.github.attilabecsvardi.pega.jdbc.PegaDriver) {
                    break searchInstanceOf;
                }
            }
            fail("Driver has not been found in DriverManager's list but it should be registered");
        }

        // Deregister the driver
        PegaDriver.unload();
        assertFalse(PegaDriver.isRegistered());

        drivers = Collections.list(DriverManager.getDrivers());
        for (java.sql.Driver driver : drivers) {
            if (driver instanceof io.github.attilabecsvardi.pega.jdbc.PegaDriver) {
                fail("Driver should be deregistered but it is still present in DriverManager's list");
            }
        }

        // register again the driver
        PegaDriver.load();
        assertTrue(PegaDriver.isRegistered());

        drivers = Collections.list(DriverManager.getDrivers());
        for (java.sql.Driver driver : drivers) {
            if (driver instanceof io.github.attilabecsvardi.pega.jdbc.PegaDriver) {
                return;
            }
        }
        fail("Driver has not been found in DriverManager's list but it should be registered");
    }

    @Test
    @Disabled("already tested")
    void acceptsURL() {
    }

    @Test
    void testURL_Invalid() throws Exception {
        java.sql.Driver instance = PegaDriver.load();
        SQLException e;
        e = assertThrows(SQLException.class,
                () -> instance.acceptsURL(null));
        assertEquals("URL_FORMAT_ERROR", e.getMessage());

        e = assertThrows(SQLException.class,
                () -> instance.connect(null, config));
        assertEquals("URL_FORMAT_ERROR", e.getMessage());
        assertNull(instance.connect("jdbc:unknown", config));
    }

    @Test
    void testURL_Valid() throws Exception {
        java.sql.Driver instance = PegaDriver.load();
        assertTrue(instance.acceptsURL("jdbc:pega:"));
    }

    @Test
    void getPropertyInfo() {
        fail("Not Supported");
    }

    @Test
    void getMajorVersion() {
    }

    @Test
    void getMinorVersion() {
    }

    @Test
    void jdbcCompliant() throws Exception {
        java.sql.Driver instance = PegaDriver.load();
        assertFalse(instance.jdbcCompliant());
    }

    @Test
    void getParentLogger() {
        fail("Not Supported");
    }
}