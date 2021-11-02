package io.github.attilabecsvardi.pega.jdbc;

import io.github.attilabecsvardi.pega.jdbc.restAPI.JDBCMethod;
import io.github.attilabecsvardi.pega.jdbc.restAPI.MethodResponse;
import io.github.attilabecsvardi.pega.jdbc.restAPI.Parameter;
import io.github.attilabecsvardi.pega.jdbc.restAPI.RestClient;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.util.*;

import static io.github.attilabecsvardi.pega.jdbc.Utils.callRemoteMethod;

/**
 * Represents a result set.
 * <p>
 * ??????
 * Column labels are case-insensitive, quotes are not supported. The first
 * column has the column index 1.
 * </p>
 * <p>
 * ????????????????
 * Thread safety: the result set is not thread-safe and must not be used by
 * multiple threads concurrently.
 * </p>
 * <p>
 * ????????????????
 * Updatable result sets: Result sets are updatable when the result only
 * contains columns from one table, and if it contains all columns of a unique
 * index (primary key or other) of this table. Key columns may not contain NULL
 * (because multiple rows with NULL could exist). In updatable result sets, own
 * changes are visible, but not own inserts and deletes.
 * </p>
 */
public class PegaResultSet implements ResultSet {

    private static final String REMOTE_INSTANCE_TYPE = "ResultSet";
    private final String GUID = "PEGA" + UUID.randomUUID().toString().replace("-", "");

    private final PegaConnection conn;
    private final PegaStatement stat;
    private final RestClient client;
    private MethodResponse mr;
    private ListIterator<LinkedHashMap> recIterator;
    private LinkedHashMap<String, String> recordHM;
    private ArrayList<String> recordAL;
    private ArrayList<String> columnList;
    private boolean wasNull;

    public PegaResultSet(PegaConnection conn, PegaStatement stat, MethodResponse mr) {
        this.conn = conn;
        this.stat = stat;
        this.client = conn.getClient();
        this.mr = mr;
        this.wasNull = false;

        init();
    }

    private void init() {
        if (mr != null && mr.getRecordList() != null) {
            recIterator = mr.getRecordList().listIterator();
        }
    }

    public String getGUID() {
        return this.GUID;
    }

    public void setMr(MethodResponse mr) {
        this.mr = mr;

        init();
    }

    /**
     * Moves the cursor forward one row from its current position.
     * A <code>ResultSet</code> cursor is initially positioned
     * before the first row; the first call to the method
     * <code>next</code> makes the first row the current row; the
     * second call makes the second row the current row, and so on.
     * <p>
     * When a call to the <code>next</code> method returns <code>false</code>,
     * the cursor is positioned after the last row. Any
     * invocation of a <code>ResultSet</code> method which requires a
     * current row will result in a <code>SQLException</code> being thrown.
     * If the result set type is <code>TYPE_FORWARD_ONLY</code>, it is vendor specified
     * whether their JDBC driver implementation will return <code>false</code> or
     * throw an <code>SQLException</code> on a
     * subsequent call to <code>next</code>.
     *
     * <P>If an input stream is open for the current row, a call
     * to the method <code>next</code> will
     * implicitly close it. A <code>ResultSet</code> object's
     * warning chain is cleared when a new row is read.
     *
     * @return <code>true</code> if the new current row is valid;
     * <code>false</code> if there are no more rows
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public boolean next() throws SQLException {
        if (recIterator != null && recIterator.hasNext()) {
            this.recordHM = (LinkedHashMap<String, String>) recIterator.next();
            this.recordAL = new ArrayList<>(recordHM.values());
            return true;
        }
        return false;
    }

    /**
     * Releases this <code>ResultSet</code> object's database and
     * JDBC resources immediately instead of waiting for
     * this to happen when it is automatically closed.
     *
     * <P>The closing of a <code>ResultSet</code> object does <strong>not</strong> close the <code>Blob</code>,
     * <code>Clob</code> or <code>NClob</code> objects created by the <code>ResultSet</code>. <code>Blob</code>,
     * <code>Clob</code> or <code>NClob</code> objects remain valid for at least the duration of the
     * transaction in which they are created, unless their <code>free</code> method is invoked.
     * <p>
     * When a <code>ResultSet</code> is closed, any <code>ResultSetMetaData</code>
     * instances that were created by calling the  <code>getMetaData</code>
     * method remain accessible.
     *
     * <P><B>Note:</B> A <code>ResultSet</code> object
     * is automatically closed by the
     * <code>Statement</code> object that generated it when
     * that <code>Statement</code> object is closed,
     * re-executed, or is used to retrieve the next result from a
     * sequence of multiple results.
     * <p>
     * Calling the method <code>close</code> on a <code>ResultSet</code>
     * object that is already closed is a no-op.
     *
     * @throws SQLException if a database access error occurs
     */
    public void close() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("close", null);
        callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
    }

    /**
     * Reports whether
     * the last column read had a value of SQL <code>NULL</code>.
     * Note that you must first call one of the getter methods
     * on a column to try to read its value and then call
     * the method <code>wasNull</code> to see if the value read was
     * SQL <code>NULL</code>.
     *
     * @return <code>true</code> if the last column value read was SQL
     * <code>NULL</code> and <code>false</code> otherwise
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public boolean wasNull() throws SQLException {
        return wasNull;
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public String getString(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return recordAL.get(columnIndex - 1);
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e.getMessage());
        }

    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>boolean</code> in the Java programming language.
     *
     * <P>If the designated column has a datatype of CHAR or VARCHAR
     * and contains a "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT
     * and contains  a 0, a value of <code>false</code> is returned.  If the designated column has a datatype
     * of CHAR or VARCHAR
     * and contains a "1" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT
     * and contains  a 1, a value of <code>true</code> is returned.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>false</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public boolean getBoolean(int columnIndex) throws SQLException {
        if (recordAL.get(columnIndex - 1) != null) {
            this.wasNull = false;
            //return Boolean.parseBoolean(recordAL.get(columnIndex - 1));
            String value = recordAL.get(columnIndex - 1).trim();
            return "1".equalsIgnoreCase(value) ||
                    "yes".equalsIgnoreCase(value) ||
                    "on".equalsIgnoreCase(value) ||
                    "true".equalsIgnoreCase(value);
        } else {
            wasNull = true;
            return false;
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public byte getByte(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("byte getByte(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>short</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public short getShort(int columnIndex) throws SQLException {
        if (recordAL.get(columnIndex - 1) != null) {
            wasNull = false;
            return Short.parseShort(recordAL.get(columnIndex - 1));
        } else {
            wasNull = true;
            return 0;
        }

    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>int</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public int getInt(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Integer.parseInt(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException ne) {
            throw new SQLException(ne);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>long</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public long getLong(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Long.parseLong(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>float</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public float getFloat(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Float.parseFloat(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>double</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public double getDouble(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Double.parseDouble(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.BigDecimal</code> in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param scale       the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @deprecated Use {@code getBigDecimal(int columnIndex)}
     * or {@code getBigDecimal(String columnLabel)}
     */
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return new BigDecimal(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return null;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> array in the Java programming language.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public byte[] getBytes(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("byte[] getBytes(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Date getDate(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Date.valueOf(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Time getTime(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Time.valueOf(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return Timestamp.valueOf(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a stream of ASCII characters. The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code> values.
     * The JDBC driver will
     * do any necessary conversion from the database format into ASCII.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.  Also, a
     * stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of one-byte ASCII characters;
     * if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getAsciiStream(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * as a stream of two-byte 3 characters. The first byte is
     * the high byte; the second byte is the low byte.
     * <p>
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code>values.  The
     * JDBC driver will do any necessary conversion from the database
     * format into Unicode.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.
     * Also, a stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called, whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of two-byte Unicode characters;
     * if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code>
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @deprecated use <code>getCharacterStream</code> in place of
     * <code>getUnicodeStream</code>
     */
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getUnicodeStream(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a  stream of
     * uninterpreted bytes. The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code> values.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.  Also, a
     * stream may return <code>0</code> when the method
     * <code>InputStream.available</code>
     * is called whether there is data available or not.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value
     * as a stream of uninterpreted bytes;
     * if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code>
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getBinaryStream(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public String getString(String columnLabel) throws SQLException {
        //Object o
        if (recordHM.get(columnLabel) != null) {
            wasNull = false;
            return recordHM.get(columnLabel);
        } else {
            wasNull = true;
            return null;
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>boolean</code> in the Java programming language.
     *
     * <P>If the designated column has a datatype of CHAR or VARCHAR
     * and contains a "0" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT
     * and contains  a 0, a value of <code>false</code> is returned.  If the designated column has a datatype
     * of CHAR or VARCHAR
     * and contains a "1" or has a datatype of BIT, TINYINT, SMALLINT, INTEGER or BIGINT
     * and contains  a 1, a value of <code>true</code> is returned.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>false</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public boolean getBoolean(String columnLabel) throws SQLException {
        if (recordHM.get(columnLabel) != null) {
            wasNull = false;
            return Boolean.parseBoolean(recordHM.get(columnLabel));
        } else {
            wasNull = true;
            return false;
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public byte getByte(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("byte getByte(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>short</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public short getShort(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Short.parseShort(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>int</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public int getInt(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                return Integer.parseInt(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>long</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public long getLong(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Long.parseLong(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>float</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public float getFloat(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Float.parseFloat(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>double</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>0</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public double getDouble(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Double.parseDouble(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return 0;
            }
        } catch (NumberFormatException e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.math.BigDecimal</code> in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param scale       the number of digits to the right of the decimal point
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @deprecated Use {@code getBigDecimal(int columnIndex)}
     * or {@code getBigDecimal(String columnLabel)}
     */
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return new BigDecimal(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>byte</code> array in the Java programming language.
     * The bytes represent the raw values returned by the driver.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public byte[] getBytes(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("byte[] getBytes(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Date</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Date getDate(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Date.valueOf(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Time</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Time getTime(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Time.valueOf(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>java.sql.Timestamp</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return Timestamp.valueOf(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of
     * ASCII characters. The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code> values.
     * The JDBC driver will
     * do any necessary conversion from the database format into ASCII.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream. Also, a
     * stream may return <code>0</code> when the method <code>available</code>
     * is called whether there is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of one-byte ASCII characters.
     * If the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code>.
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getAsciiStream(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of two-byte
     * Unicode characters. The first byte is the high byte; the second
     * byte is the low byte.
     * <p>
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARCHAR</code> values.
     * The JDBC technology-enabled driver will
     * do any necessary conversion from the database format into Unicode.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream.
     * Also, a stream may return <code>0</code> when the method
     * <code>InputStream.available</code> is called, whether there
     * is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of two-byte Unicode characters.
     * If the value is SQL <code>NULL</code>, the value returned
     * is <code>null</code>.
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @deprecated use <code>getCharacterStream</code> instead
     */
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getUnicodeStream(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a stream of uninterpreted
     * <code>byte</code>s.
     * The value can then be read in chunks from the
     * stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code>
     * values.
     *
     * <P><B>Note:</B> All the data in the returned stream must be
     * read prior to getting the value of any other column. The next
     * call to a getter method implicitly closes the stream. Also, a
     * stream may return <code>0</code> when the method <code>available</code>
     * is called whether there is data available or not.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a Java input stream that delivers the database column value
     * as a stream of uninterpreted bytes;
     * if the value is SQL <code>NULL</code>, the result is <code>null</code>
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream getBinaryStream(String columnLabel)");
    }

    /**
     * Retrieves the first warning reported by calls on this
     * <code>ResultSet</code> object.
     * Subsequent warnings on this <code>ResultSet</code> object
     * will be chained to the <code>SQLWarning</code> object that
     * this method returns.
     *
     * <P>The warning chain is automatically cleared each time a new
     * row is read.  This method may not be called on a <code>ResultSet</code>
     * object that has been closed; doing so will cause an
     * <code>SQLException</code> to be thrown.
     * <p>
     * <B>Note:</B> This warning chain only covers warnings caused
     * by <code>ResultSet</code> methods.  Any warning caused by
     * <code>Statement</code> methods
     * (such as reading OUT parameters) will be chained on the
     * <code>Statement</code> object.
     *
     * @return the first <code>SQLWarning</code> object reported or
     * <code>null</code> if there are none
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public SQLWarning getWarnings() throws SQLException {
        // throw new SQLFeatureNotSupportedException("SQLWarning getWarnings()");
        // call method on the server side
        JDBCMethod method = new JDBCMethod("getWarnings", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        if (mr != null) {
            return new SQLWarning(mr.getReturnValue());
        } else {
            return null;
        }

    }

    /**
     * Clears all warnings reported on this <code>ResultSet</code> object.
     * After this method is called, the method <code>getWarnings</code>
     * returns <code>null</code> until a new warning is
     * reported for this <code>ResultSet</code> object.
     *
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public void clearWarnings() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("clearWarnings", null);
        callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
    }

    /**
     * Retrieves the name of the SQL cursor used by this <code>ResultSet</code>
     * object.
     *
     * <P>In SQL, a result table is retrieved through a cursor that is
     * named. The current row of a result set can be updated or deleted
     * using a positioned update/delete statement that references the
     * cursor name. To insure that the cursor has the proper isolation
     * level to support update, the cursor's <code>SELECT</code> statement
     * should be of the form <code>SELECT FOR UPDATE</code>. If
     * <code>FOR UPDATE</code> is omitted, the positioned updates may fail.
     *
     * <P>The JDBC API supports this SQL feature by providing the name of the
     * SQL cursor used by a <code>ResultSet</code> object.
     * The current row of a <code>ResultSet</code> object
     * is also the current row of this SQL cursor.
     *
     * @return the SQL name for this <code>ResultSet</code> object's cursor
     * @throws SQLException                    if a database access error occurs or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     */
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException("String getCursorName()");
    }

    /**
     * Retrieves the  number, types and properties of
     * this <code>ResultSet</code> object's columns.
     *
     * @return the description of this <code>ResultSet</code> object's columns
     * @throws SQLException if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public ResultSetMetaData getMetaData() throws SQLException {
        PegaResultSetMetaData ret = new PegaResultSetMetaData(conn, null);
        MethodResponse mr;
        // create a ResultSetMetaData object on the server side
        JDBCMethod method = new JDBCMethod("getMetaData", null, ret.getGUID());

        mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);

        if (mr != null) {
            ret.setMr(mr);
            return ret;
        } else {
            return null;
        }
    }

    /**
     * <p>Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java object type corresponding to the column's SQL type,
     * following the mapping for built-in types specified in the JDBC
     * specification. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     *
     * <p>This method may also be used to read database-specific
     * abstract data types.
     * <p>
     * In the JDBC 2.0 API, the behavior of method
     * <code>getObject</code> is extended to materialize
     * data of SQL user-defined types.
     * <p>
     * If <code>Connection.getTypeMap</code> does not throw a
     * <code>SQLFeatureNotSupportedException</code>,
     * then when a column contains a structured or distinct value,
     * the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     * <p>
     * If <code>Connection.getTypeMap</code> does throw a
     * <code>SQLFeatureNotSupportedException</code>,
     * then structured values are not supported, and distinct values
     * are mapped to the default Java class as determined by the
     * underlying SQL type of the DISTINCT type.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Object getObject(int columnIndex) throws SQLException {
        // only a temp solution, type check should be done
        return getString(columnIndex);
        //throw new SQLFeatureNotSupportedException("Object getObject(int columnIndex)");
    }

    /**
     * <p>Gets the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * an <code>Object</code> in the Java programming language.
     *
     * <p>This method will return the value of the given column as a
     * Java object.  The type of the Java object will be the default
     * Java object type corresponding to the column's SQL type,
     * following the mapping for built-in types specified in the JDBC
     * specification. If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * <p>
     * This method may also be used to read database-specific
     * abstract data types.
     * <p>
     * In the JDBC 2.0 API, the behavior of the method
     * <code>getObject</code> is extended to materialize
     * data of SQL user-defined types.  When a column contains
     * a structured or distinct value, the behavior of this method is as
     * if it were a call to: <code>getObject(columnIndex,
     * this.getStatement().getConnection().getTypeMap())</code>.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>java.lang.Object</code> holding the column value
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     */
    public Object getObject(String columnLabel) throws SQLException {
        // only a temp solution, type check should be done
        return getString(columnLabel);
        //throw new SQLFeatureNotSupportedException("Object getObject(String columnLabel)");
    }

    /**
     * Maps the given <code>ResultSet</code> column label to its
     * <code>ResultSet</code> column index.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column index of the given column name
     * @throws SQLException if the <code>ResultSet</code> object
     *                      does not contain a column labeled <code>columnLabel</code>, a database access error occurs
     *                      or this method is called on a closed result set
     */
    public int findColumn(String columnLabel) throws SQLException {

        if (columnList == null) {
            columnList = new ArrayList<>();
            ResultSetMetaData meta = getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                columnList.add(meta.getColumnLabel(i).toUpperCase());
            }
        }

        int i;

        if (columnList != null) {
            i = columnList.indexOf(columnLabel.toUpperCase());
        } else {
            throw new SQLException("No available column list");
        }

        if (i == -1) {
            throw new SQLException("Column not found, columnLabel: " + columnLabel);
        }

        return i + 1;
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the column
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     * @since 1.2
     */
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader getCharacterStream(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     * @since 1.2
     */
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader getCharacterStream(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value (full precision);
     * if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     * @since 1.2
     */
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        try {
            if (recordAL.get(columnIndex - 1) != null) {
                wasNull = false;
                return new BigDecimal(recordAL.get(columnIndex - 1));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.math.BigDecimal</code> with full precision.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value (full precision);
     * if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs or this method is
     *                      called on a closed result set
     * @since 1.2
     */
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        try {
            if (recordHM.get(columnLabel) != null) {
                wasNull = false;
                return new BigDecimal(recordHM.get(columnLabel));
            } else {
                wasNull = true;
                return null;
            }
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    /**
     * Retrieves whether the cursor is before the first row in
     * this <code>ResultSet</code> object.
     * <p>
     * <strong>Note:</strong>Support for the <code>isBeforeFirst</code> method
     * is optional for <code>ResultSet</code>s with a result
     * set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is before the first row;
     * <code>false</code> if the cursor is at any other position or the
     * result set contains no rows
     * @throws SQLException                    if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean isBeforeFirst() throws SQLException {
        return recIterator.nextIndex() < 1 && recIterator.hasNext();
    }

    /**
     * Retrieves whether the cursor is after the last row in
     * this <code>ResultSet</code> object.
     * <p>
     * <strong>Note:</strong>Support for the <code>isAfterLast</code> method
     * is optional for <code>ResultSet</code>s with a result
     * set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is after the last row;
     * <code>false</code> if the cursor is at any other position or the
     * result set contains no rows
     * @throws SQLException                    if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean isAfterLast()");
    }

    /**
     * Retrieves whether the cursor is on the first row of
     * this <code>ResultSet</code> object.
     * <p>
     * <strong>Note:</strong>Support for the <code>isFirst</code> method
     * is optional for <code>ResultSet</code>s with a result
     * set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is on the first row;
     * <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean isFirst() throws SQLException {
        return recIterator.nextIndex() == 1;
    }

    /**
     * Retrieves whether the cursor is on the last row of
     * this <code>ResultSet</code> object.
     * <strong>Note:</strong> Calling the method <code>isLast</code> may be expensive
     * because the JDBC driver
     * might need to fetch ahead one row in order to determine
     * whether the current row is the last row in the result set.
     * <p>
     * <strong>Note:</strong> Support for the <code>isLast</code> method
     * is optional for <code>ResultSet</code>s with a result
     * set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return <code>true</code> if the cursor is on the last row;
     * <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs or this method is
     *                                         called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean isLast()");
    }

    /**
     * Moves the cursor to the front of
     * this <code>ResultSet</code> object, just before the
     * first row. This method has no effect if the result set contains no rows.
     *
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set or the
     *                                         result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException("void beforeFirst()");
    }

    /**
     * Moves the cursor to the end of
     * this <code>ResultSet</code> object, just after the
     * last row. This method has no effect if the result set contains no rows.
     *
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set
     *                                         or the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException("void afterLast()");
    }

    /**
     * Moves the cursor to the first row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     * <code>false</code> if there are no rows in the result set
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set
     *                                         or the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean first() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean first()");
    }

    /**
     * Moves the cursor to the last row in
     * this <code>ResultSet</code> object.
     *
     * @return <code>true</code> if the cursor is on a valid row;
     * <code>false</code> if there are no rows in the result set
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set
     *                                         or the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean last()");
    }

    /**
     * Retrieves the current row number.  The first row is number 1, the
     * second number 2, and so on.
     * <p>
     * <strong>Note:</strong>Support for the <code>getRow</code> method
     * is optional for <code>ResultSet</code>s with a result
     * set type of <code>TYPE_FORWARD_ONLY</code>
     *
     * @return the current row number; <code>0</code> if there is no current row
     * @throws SQLException                    if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("int getRow()");
    }

    /**
     * Moves the cursor to the given row number in
     * this <code>ResultSet</code> object.
     *
     * <p>If the row number is positive, the cursor moves to
     * the given row number with respect to the
     * beginning of the result set.  The first row is row 1, the second
     * is row 2, and so on.
     *
     * <p>If the given row number is negative, the cursor moves to
     * an absolute row position with respect to
     * the end of the result set.  For example, calling the method
     * <code>absolute(-1)</code> positions the
     * cursor on the last row; calling the method <code>absolute(-2)</code>
     * moves the cursor to the next-to-last row, and so on.
     *
     * <p>If the row number specified is zero, the cursor is moved to
     * before the first row.
     *
     * <p>An attempt to position the cursor beyond the first/last row in
     * the result set leaves the cursor before the first row or after
     * the last row.
     *
     * <p><B>Note:</B> Calling <code>absolute(1)</code> is the same
     * as calling <code>first()</code>. Calling <code>absolute(-1)</code>
     * is the same as calling <code>last()</code>.
     *
     * @param row the number of the row to which the cursor should move.
     *            A value of zero indicates that the cursor will be positioned
     *            before the first row; a positive number indicates the row number
     *            counting from the beginning of the result set; a negative number
     *            indicates the row number counting from the end of the result set
     * @return <code>true</code> if the cursor is moved to a position in this
     * <code>ResultSet</code> object;
     * <code>false</code> if the cursor is before the first row or after the
     * last row
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set
     *                                         or the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean absolute(int row)");
    }

    /**
     * Moves the cursor a relative number of rows, either positive or negative.
     * Attempting to move beyond the first/last row in the
     * result set positions the cursor before/after the
     * the first/last row. Calling <code>relative(0)</code> is valid, but does
     * not change the cursor position.
     *
     * <p>Note: Calling the method <code>relative(1)</code>
     * is identical to calling the method <code>next()</code> and
     * calling the method <code>relative(-1)</code> is identical
     * to calling the method <code>previous()</code>.
     *
     * @param rows an <code>int</code> specifying the number of rows to
     *             move from the current row; a positive number moves the cursor
     *             forward; a negative number moves the cursor backward
     * @return <code>true</code> if the cursor is on a row;
     * <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs;  this method
     *                                         is called on a closed result set or the result set type is
     *                                         <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean relative(int rows)");
    }

    /**
     * Moves the cursor to the previous row in this
     * <code>ResultSet</code> object.
     * <p>
     * When a call to the <code>previous</code> method returns <code>false</code>,
     * the cursor is positioned before the first row.  Any invocation of a
     * <code>ResultSet</code> method which requires a current row will result in a
     * <code>SQLException</code> being thrown.
     * <p>
     * If an input stream is open for the current row, a call to the method
     * <code>previous</code> will implicitly close it.  A <code>ResultSet</code>
     * object's warning change is cleared when a new row is read.
     * <p>
     *
     * @return <code>true</code> if the cursor is now positioned on a valid row;
     * <code>false</code> if the cursor is positioned before the first row
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set
     *                                         or the result set type is <code>TYPE_FORWARD_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean previous()");
    }

    /**
     * Retrieves the fetch direction for this
     * <code>ResultSet</code> object.
     *
     * @return the current fetch direction for this <code>ResultSet</code> object
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @see #setFetchDirection
     * @since 1.2
     */
    public int getFetchDirection() throws SQLException {
        //throw new SQLFeatureNotSupportedException("int getFetchDirection()");

        // call method on the server side
        JDBCMethod method = new JDBCMethod("getFetchDirection", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Integer.parseInt(mr.getReturnValue());
    }

    /**
     * Gives a hint as to the direction in which the rows in this
     * <code>ResultSet</code> object will be processed.
     * The initial value is determined by the
     * <code>Statement</code> object
     * that produced this <code>ResultSet</code> object.
     * The fetch direction may be changed at any time.
     *
     * @param direction an <code>int</code> specifying the suggested
     *                  fetch direction; one of <code>ResultSet.FETCH_FORWARD</code>,
     *                  <code>ResultSet.FETCH_REVERSE</code>, or
     *                  <code>ResultSet.FETCH_UNKNOWN</code>
     * @throws SQLException if a database access error occurs; this
     *                      method is called on a closed result set or
     *                      the result set type is <code>TYPE_FORWARD_ONLY</code> and the fetch
     *                      direction is not <code>FETCH_FORWARD</code>
     * @see Statement#setFetchDirection
     * @see #getFetchDirection
     * @since 1.2
     */
    public void setFetchDirection(int direction) throws SQLException {
        throw new SQLFeatureNotSupportedException("void setFetchDirection(int direction)");
    }

    /**
     * Retrieves the fetch size for this
     * <code>ResultSet</code> object.
     *
     * @return the current fetch size for this <code>ResultSet</code> object
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @see #setFetchSize
     * @since 1.2
     */
    public int getFetchSize() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("getFetchSize", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Integer.parseInt(mr.getReturnValue());
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should
     * be fetched from the database when more rows are needed for this
     * <code>ResultSet</code> object.
     * If the fetch size specified is zero, the JDBC driver
     * ignores the value and is free to make its own best guess as to what
     * the fetch size should be.  The default value is set by the
     * <code>Statement</code> object
     * that created the result set.  The fetch size may be changed at any time.
     *
     * @param rows the number of rows to fetch
     * @throws SQLException if a database access error occurs; this method
     *                      is called on a closed result set or the
     *                      condition {@code rows >= 0} is not satisfied
     * @see #getFetchSize
     * @since 1.2
     */
    public void setFetchSize(int rows) throws SQLException {
        // call method on the server side
        ArrayList<Parameter> paramList = new ArrayList<>();
        paramList.add(new Parameter("int", String.valueOf(rows)));
        JDBCMethod method = new JDBCMethod("setFetchSize", paramList);
        callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
    }

    /**
     * Retrieves the type of this <code>ResultSet</code> object.
     * The type is determined by the <code>Statement</code> object
     * that created the result set.
     *
     * @return <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     * <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>,
     * or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public int getType() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("getType", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Integer.parseInt(mr.getReturnValue());
    }

    /**
     * Retrieves the concurrency mode of this <code>ResultSet</code> object.
     * The concurrency used is determined by the
     * <code>Statement</code> object that created the result set.
     *
     * @return the concurrency type, either
     * <code>ResultSet.CONCUR_READ_ONLY</code>
     * or <code>ResultSet.CONCUR_UPDATABLE</code>
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public int getConcurrency() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("getConcurrency", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Integer.parseInt(mr.getReturnValue());
    }

    /**
     * Retrieves whether the current row has been updated.  The value returned
     * depends on whether or not the result set can detect updates.
     * <p>
     * <strong>Note:</strong> Support for the <code>rowUpdated</code> method is optional with a result set
     * concurrency of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to
     * have been visibly updated by the owner or another; <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see DatabaseMetaData#updatesAreDetected
     * @since 1.2
     */
    public boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean rowUpdated()");
    }

    /**
     * Retrieves whether the current row has had an insertion.
     * The value returned depends on whether or not this
     * <code>ResultSet</code> object can detect visible inserts.
     * <p>
     * <strong>Note:</strong> Support for the <code>rowInserted</code> method is optional with a result set
     * concurrency of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to
     * have been inserted; <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see DatabaseMetaData#insertsAreDetected
     * @since 1.2
     */
    public boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean rowInserted()");
    }

    /**
     * Retrieves whether a row has been deleted.  A deleted row may leave
     * a visible "hole" in a result set.  This method can be used to
     * detect holes in a result set.  The value returned depends on whether
     * or not this <code>ResultSet</code> object can detect deletions.
     * <p>
     * <strong>Note:</strong> Support for the <code>rowDeleted</code> method is optional with a result set
     * concurrency of <code>CONCUR_READ_ONLY</code>
     *
     * @return <code>true</code> if the current row is detected to
     * have been deleted by the owner or another; <code>false</code> otherwise
     * @throws SQLException                    if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @see DatabaseMetaData#deletesAreDetected
     * @since 1.2
     */
    public boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean rowDeleted()");
    }

    /**
     * Updates the designated column with a <code>null</code> value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code>
     * or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNull(int columnIndex)");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBoolean(int columnIndex, boolean x)");
    }

    /**
     * Updates the designated column with a <code>byte</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateByte(int columnIndex, byte x)");
    }

    /**
     * Updates the designated column with a <code>short</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateShort(int columnIndex, short x)");
    }

    /**
     * Updates the designated column with an <code>int</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateInt(int columnIndex, int x)");
    }

    /**
     * Updates the designated column with a <code>long</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateLong(int columnIndex, long x)");
    }

    /**
     * Updates the designated column with a <code>float</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateFloat(int columnIndex, float x)");
    }

    /**
     * Updates the designated column with a <code>double</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateDouble(int columnIndex, double x)");
    }

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code>
     * value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBigDecimal(int columnIndex, BigDecimal x)");
    }

    /**
     * Updates the designated column with a <code>String</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateString(int columnIndex, String x)");
    }

    /**
     * Updates the designated column with a <code>byte</code> array value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBytes(int columnIndex, byte[] x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateDate(int columnIndex, Date x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateTime(int columnIndex, Time x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code>
     * value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateTimestamp(int columnIndex, Timestamp x)");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(int columnIndex, InputStream x, int length)");
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(int columnIndex, InputStream x, int length)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(int columnIndex, Reader x, int length)");
    }

    /**
     * Updates the designated column with an <code>Object</code> value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * <p>
     * If the second argument is an <code>InputStream</code> then the stream must contain
     * the number of bytes specified by scaleOrLength.  If the second argument is a
     * <code>Reader</code> then the reader must contain the number of characters specified
     * by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the statement is executed.
     *
     * @param columnIndex   the first column is 1, the second is 2, ...
     * @param x             the new column value
     * @param scaleOrLength for an object of <code>java.math.BigDecimal</code> ,
     *                      this is the number of digits after the decimal point. For
     *                      Java Object types <code>InputStream</code> and <code>Reader</code>,
     *                      this is the length
     *                      of the data in the stream or reader.  For all other types,
     *                      this value will be ignored.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateObject(int columnIndex, Object x, int scaleOrLength)");
    }

    /**
     * Updates the designated column with an <code>Object</code> value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateObject(int columnIndex, Object x)");
    }

    /**
     * Updates the designated column with a <code>null</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateNull(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNull(String columnLabel)");
    }

    /**
     * Updates the designated column with a <code>boolean</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBoolean(String columnLabel, boolean x)");
    }

    /**
     * Updates the designated column with a <code>byte</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateByte(String columnLabel, byte x)");
    }

    /**
     * Updates the designated column with a <code>short</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateShort(String columnLabel, short x)");
    }

    /**
     * Updates the designated column with an <code>int</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateInt(String columnLabel, int x)");
    }

    /**
     * Updates the designated column with a <code>long</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateLong(String columnLabel, long x)");
    }

    /**
     * Updates the designated column with a <code>float </code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateFloat(String columnLabel, float x)");
    }

    /**
     * Updates the designated column with a <code>double</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateDouble(String columnLabel, double x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code>
     * value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBigDecimal(String columnLabel, BigDecimal x)");
    }

    /**
     * Updates the designated column with a <code>String</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateString(String columnLabel, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateString(String columnLabel, String x)");
    }

    /**
     * Updates the designated column with a byte array value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code>
     * or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBytes(String columnLabel, byte[] x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateDate(String columnLabel, Date x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateTime(String columnLabel, Time x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code>
     * value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateTimestamp(String columnLabel, Timestamp x)");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(String columnLabel, InputStream x, int length)");
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(String columnLabel, InputStream x, int length)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing
     *                    the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(String columnLabel, Reader reader, int length)");
    }

    /**
     * Updates the designated column with an <code>Object</code> value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     * <p>
     * If the second argument is an <code>InputStream</code> then the stream must contain
     * the number of bytes specified by scaleOrLength.  If the second argument is a
     * <code>Reader</code> then the reader must contain the number of characters specified
     * by scaleOrLength. If these conditions are not true the driver will generate a
     * <code>SQLException</code> when the statement is executed.
     *
     * @param columnLabel   the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x             the new column value
     * @param scaleOrLength for an object of <code>java.math.BigDecimal</code> ,
     *                      this is the number of digits after the decimal point. For
     *                      Java Object types <code>InputStream</code> and <code>Reader</code>,
     *                      this is the length
     *                      of the data in the stream or reader.  For all other types,
     *                      this value will be ignored.
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateObject(String columnLabel, Object x, int scaleOrLength)");
    }

    /**
     * Updates the designated column with an <code>Object</code> value.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateObject(String columnLabel, Object x)");
    }

    /**
     * Inserts the contents of the insert row into this
     * <code>ResultSet</code> object and into the database.
     * The cursor must be on the insert row when this method is called.
     *
     * @throws SQLException                    if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>,
     *                                         this method is called on a closed result set,
     *                                         if this method is called when the cursor is not on the insert row,
     *                                         or if not all of non-nullable columns in
     *                                         the insert row have been given a non-null value
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void insertRow()");
    }

    /**
     * Updates the underlying database with the new contents of the
     * current row of this <code>ResultSet</code> object.
     * This method cannot be called when the cursor is on the insert row.
     *
     * @throws SQLException                    if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>;
     *                                         this method is called on a closed result set or
     *                                         if this method is called when the cursor is on the insert row
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateRow()");
    }

    /**
     * Deletes the current row from this <code>ResultSet</code> object
     * and from the underlying database.  This method cannot be called when
     * the cursor is on the insert row.
     *
     * @throws SQLException                    if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>;
     *                                         this method is called on a closed result set
     *                                         or if this method is called when the cursor is on the insert row
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void deleteRow()");
    }

    /**
     * Refreshes the current row with its most recent value in
     * the database.  This method cannot be called when
     * the cursor is on the insert row.
     *
     * <P>The <code>refreshRow</code> method provides a way for an
     * application to
     * explicitly tell the JDBC driver to refetch a row(s) from the
     * database.  An application may want to call <code>refreshRow</code> when
     * caching or prefetching is being done by the JDBC driver to
     * fetch the latest value of a row from the database.  The JDBC driver
     * may actually refresh multiple rows at once if the fetch size is
     * greater than one.
     *
     * <P> All values are refetched subject to the transaction isolation
     * level and cursor sensitivity.  If <code>refreshRow</code> is called after
     * calling an updater method, but before calling
     * the method <code>updateRow</code>, then the
     * updates made to the row are lost.  Calling the method
     * <code>refreshRow</code> frequently will likely slow performance.
     *
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set;
     *                                         the result set type is <code>TYPE_FORWARD_ONLY</code> or if this
     *                                         method is called when the cursor is on the insert row
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method or this method is not supported for the specified result
     *                                         set type and result set concurrency.
     * @since 1.2
     */
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void refreshRow()");
    }

    /**
     * Cancels the updates made to the current row in this
     * <code>ResultSet</code> object.
     * This method may be called after calling an
     * updater method(s) and before calling
     * the method <code>updateRow</code> to roll back
     * the updates made to a row.  If no updates have been made or
     * <code>updateRow</code> has already been called, this method has no
     * effect.
     *
     * @throws SQLException                    if a database access error
     *                                         occurs; this method is called on a closed result set;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or if this method is called when the cursor is
     *                                         on the insert row
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("void cancelRowUpdates()");
    }

    /**
     * Moves the cursor to the insert row.  The current cursor position is
     * remembered while the cursor is positioned on the insert row.
     * <p>
     * The insert row is a special row associated with an updatable
     * result set.  It is essentially a buffer where a new row may
     * be constructed by calling the updater methods prior to
     * inserting the row into the result set.
     * <p>
     * Only the updater, getter,
     * and <code>insertRow</code> methods may be
     * called when the cursor is on the insert row.  All of the columns in
     * a result set must be given a value each time this method is
     * called before calling <code>insertRow</code>.
     * An updater method must be called before a
     * getter method can be called on a column value.
     *
     * @throws SQLException                    if a database access error occurs; this
     *                                         method is called on a closed result set
     *                                         or the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void moveToInsertRow()");
    }

    /**
     * Moves the cursor to the remembered cursor position, usually the
     * current row.  This method has no effect if the cursor is not on
     * the insert row.
     *
     * @throws SQLException                    if a database access error occurs; this
     *                                         method is called on a closed result set
     *                                         or the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("void moveToCurrentRow()");
    }

    /**
     * Retrieves the <code>Statement</code> object that produced this
     * <code>ResultSet</code> object.
     * If the result set was generated some other way, such as by a
     * <code>DatabaseMetaData</code> method, this method  may return
     * <code>null</code>.
     *
     * @return the <code>Statement</code> object that produced
     * this <code>ResultSet</code> object or <code>null</code>
     * if the result set was produced some other way
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Statement getStatement() throws SQLException {
        return stat;
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as an <code>Object</code>
     * in the Java programming language.
     * If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * This method uses the given <code>Map</code> object
     * for the custom mapping of the
     * SQL structured or distinct type that is being retrieved.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param map         a <code>java.util.Map</code> object that contains the mapping
     *                    from SQL type names to classes in the Java programming language
     * @return an <code>Object</code> in the Java programming language
     * representing the SQL value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("Object getObject(int columnIndex, Map<String, Class<?>> map)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Ref</code> object
     * in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Ref</code> object representing an SQL <code>REF</code>
     * value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref getRef(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Blob</code> object
     * in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Blob</code> object representing the SQL
     * <code>BLOB</code> value in the specified column
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob getBlob(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Clob</code> object
     * in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>Clob</code> object representing the SQL
     * <code>CLOB</code> value in the specified column
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob getClob(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as an <code>Array</code> object
     * in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return an <code>Array</code> object representing the SQL
     * <code>ARRAY</code> value in the specified column
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array getArray(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as an <code>Object</code>
     * in the Java programming language.
     * If the value is an SQL <code>NULL</code>,
     * the driver returns a Java <code>null</code>.
     * This method uses the specified <code>Map</code> object for
     * custom mapping if appropriate.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param map         a <code>java.util.Map</code> object that contains the mapping
     *                    from SQL type names to classes in the Java programming language
     * @return an <code>Object</code> representing the SQL value in the
     * specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("Object getObject(String columnLabel, Map<String, Class<?>> map)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Ref</code> object
     * in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>Ref</code> object representing the SQL <code>REF</code>
     * value in the specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Ref getRef(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Ref getRef(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Blob</code> object
     * in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>Blob</code> object representing the SQL <code>BLOB</code>
     * value in the specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Blob getBlob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob getBlob(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>Clob</code> object
     * in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>Clob</code> object representing the SQL <code>CLOB</code>
     * value in the specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Clob getClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob getClob(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as an <code>Array</code> object
     * in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return an <code>Array</code> object representing the SQL <code>ARRAY</code> value in
     * the specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.2
     */
    public Array getArray(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array getArray(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the date if the underlying database does not store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the date
     * @return the column value as a <code>java.sql.Date</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date getDate(int columnIndex, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Date</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the date if the underlying database does not store
     * timezone information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the date
     * @return the column value as a <code>java.sql.Date</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Date getDate(String columnLabel, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the time if the underlying database does not store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the time
     * @return the column value as a <code>java.sql.Time</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time getTime(int columnIndex, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Time</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the time if the underlying database does not store
     * timezone information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the time
     * @return the column value as a <code>java.sql.Time</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnLabel is not valid;
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Time getTime(String columnLabel, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Timestamp</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the timestamp if the underlying database does not store
     * timezone information.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the timestamp
     * @return the column value as a <code>java.sql.Timestamp</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnIndex is not valid;
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timestamp getTimestamp(int columnIndex, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.sql.Timestamp</code> object
     * in the Java programming language.
     * This method uses the given calendar to construct an appropriate millisecond
     * value for the timestamp if the underlying database does not store
     * timezone information.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param cal         the <code>java.util.Calendar</code> object
     *                    to use in constructing the date
     * @return the column value as a <code>java.sql.Timestamp</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException if the columnLabel is not valid or
     *                      if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.2
     */
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("Timestamp getTimestamp(String columnLabel, Calendar cal)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.net.URL</code>
     * object in the Java programming language.
     *
     * @param columnIndex the index of the column 1 is the first, 2 is the second,...
     * @return the column value as a <code>java.net.URL</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs; this method
     *                                         is called on a closed result set or if a URL is malformed
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL getURL(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>java.net.URL</code>
     * object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value as a <code>java.net.URL</code> object;
     * if the value is SQL <code>NULL</code>,
     * the value returned is <code>null</code> in the Java programming language
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs; this method
     *                                         is called on a closed result set or if a URL is malformed
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public URL getURL(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL getURL(String columnLabel)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateRef(int columnIndex, Ref x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Ref</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateRef(String columnLabel, Ref x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(int columnIndex, Blob x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Blob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(String columnLabel, Blob x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(int columnIndex, Clob x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Clob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(String columnLabel, Clob x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateArray(int columnIndex, Array x)");
    }

    /**
     * Updates the designated column with a <code>java.sql.Array</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.4
     */
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateArray(String columnLabel, Array x)");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.RowId</code> object in the Java
     * programming language.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @return the column value; if the value is a SQL <code>NULL</code> the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId getRowId(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row of this
     * <code>ResultSet</code> object as a <code>java.sql.RowId</code> object in the Java
     * programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value ; if the value is a SQL <code>NULL</code> the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public RowId getRowId(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId getRowId(String columnLabel)");
    }

    /**
     * Updates the designated column with a <code>RowId</code> value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called
     * to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param x           the column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateRowId(int columnIndex, RowId x)");
    }

    /**
     * Updates the designated column with a <code>RowId</code> value. The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called
     * to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateRowId(String columnLabel, RowId x)");
    }

    /**
     * Retrieves the holdability of this <code>ResultSet</code> object
     *
     * @return either <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @throws SQLException if a database access error occurs
     *                      or this method is called on a closed result set
     * @since 1.6
     */
    public int getHoldability() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("getHoldability", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Integer.parseInt(mr.getReturnValue());
    }

    /**
     * Retrieves whether this <code>ResultSet</code> object has been closed. A <code>ResultSet</code> is closed if the
     * method close has been called on it, or if it is automatically closed.
     *
     * @return true if this <code>ResultSet</code> object is closed; false if it is still open
     * @throws SQLException if a database access error occurs
     * @since 1.6
     */
    public boolean isClosed() throws SQLException {
        // call method on the server side
        JDBCMethod method = new JDBCMethod("isClosed", null);
        MethodResponse mr = callRemoteMethod(client, REMOTE_INSTANCE_TYPE, GUID, method);
        return Boolean.parseBoolean(mr.getReturnValue());
    }

    /**
     * Updates the designated column with a <code>String</code> value.
     * It is intended for use when updating <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param nString     the value for the column to be updated
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or if a database access error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNString(int columnIndex, String nString)");
    }

    /**
     * Updates the designated column with a <code>String</code> value.
     * It is intended for use when updating <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param nString     the value for the column to be updated
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         the result set concurrency is <CODE>CONCUR_READ_ONLY</code>
     *                                         or if a database access error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNString(String columnLabel, String nString)");
    }

    /**
     * Updates the designated column with a <code>java.sql.NClob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param nClob       the value for the column to be updated
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(int columnIndex, NClob nClob)");
    }

    /**
     * Updates the designated column with a <code>java.sql.NClob</code> value.
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param nClob       the value for the column to be updated
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(String columnLabel, NClob nClob)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>NClob</code> object
     * in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>NClob</code> object representing the SQL
     * <code>NCLOB</code> value in the specified column
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set
     *                                         or if a database access error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob getNClob(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a <code>NClob</code> object
     * in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>NClob</code> object representing the SQL <code>NCLOB</code>
     * value in the specified column
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set
     *                                         or if a database access error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public NClob getNClob(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob getNClob(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in  the current row of
     * this <code>ResultSet</code> as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        // throw new SQLFeatureNotSupportedException("SQLXML getSQLXML(int columnIndex)");
        return new PegaSQLXML(recordAL.get(columnIndex - 1));
    }

    /**
     * Retrieves the value of the designated column in  the current row of
     * this <code>ResultSet</code> as a
     * <code>java.sql.SQLXML</code> object in the Java programming language.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>SQLXML</code> object that maps an <code>SQL XML</code> value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML getSQLXML(String columnLabel)");
    }

    /**
     * Updates the designated column with a <code>java.sql.SQLXML</code> value.
     * The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called
     * to update the database.
     * <p>
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param xmlObject   the value for the column to be updated
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs; this method
     *                                         is called on a closed result set;
     *                                         the <code>java.xml.transform.Result</code>,
     *                                         <code>Writer</code> or <code>OutputStream</code> has not been closed
     *                                         for the <code>SQLXML</code> object;
     *                                         if there is an error processing the XML value or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>.  The <code>getCause</code> method
     *                                         of the exception may provide a more detailed exception, for example, if the
     *                                         stream does not contain valid XML.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateSQLXML(int columnIndex, SQLXML xmlObject)");
    }

    /**
     * Updates the designated column with a <code>java.sql.SQLXML</code> value.
     * The updater
     * methods are used to update column values in the current row or the insert
     * row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called
     * to update the database.
     * <p>
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param xmlObject   the column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs; this method
     *                                         is called on a closed result set;
     *                                         the <code>java.xml.transform.Result</code>,
     *                                         <code>Writer</code> or <code>OutputStream</code> has not been closed
     *                                         for the <code>SQLXML</code> object;
     *                                         if there is an error processing the XML value or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>.  The <code>getCause</code> method
     *                                         of the exception may provide a more detailed exception, for example, if the
     *                                         stream does not contain valid XML.
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateSQLXML(String columnLabel, SQLXML xmlObject)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("String getNString(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as
     * a <code>String</code> in the Java programming language.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return the column value; if the value is SQL <code>NULL</code>, the
     * value returned is <code>null</code>
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public String getNString(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("String getNString(String columnLabel)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @return a <code>java.io.Reader</code> object that contains the column
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader getNCharacterStream(int columnIndex)");
    }

    /**
     * Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object as a
     * <code>java.io.Reader</code> object.
     * It is intended for use when
     * accessing  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @return a <code>java.io.Reader</code> object that contains the column
     * value; if the value is SQL <code>NULL</code>, the value returned is
     * <code>null</code> in the Java programming language
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader getNCharacterStream(String columnLabel)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.   The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * It is intended for use when
     * updating  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateNCharacterStream(int columnIndex, Reader x, long length)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.  The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * It is intended for use when
     * updating  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing
     *                    the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateNCharacterStream(String columnLabel, Reader reader, long length)");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(int columnIndex, InputStream x, long length)");
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(int columnIndex, InputStream x, long length)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(int columnIndex, Reader x, long length)");
    }

    /**
     * Updates the designated column with an ascii stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(String columnLabel, InputStream x, long length)");
    }

    /**
     * Updates the designated column with a binary stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(String columnLabel, InputStream x, long length)");
    }

    /**
     * Updates the designated column with a character stream value, which will have
     * the specified number of bytes.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing
     *                    the new column value
     * @param length      the length of the stream
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(String columnLabel, Reader reader, long length)");
    }

    /**
     * Updates the designated column using the given input stream, which
     * will have the specified number of bytes.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param inputStream An object that contains the data to set the parameter
     *                    value to.
     * @param length      the number of bytes in the parameter data.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(int columnIndex, InputStream inputStream, long length)");
    }

    /**
     * Updates the designated column using the given input stream, which
     * will have the specified number of bytes.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param inputStream An object that contains the data to set the parameter
     *                    value to.
     * @param length      the number of bytes in the parameter data.
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(String columnLabel, InputStream inputStream, long length)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(int columnIndex, Reader reader, long length)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateClob(String columnLabel, Reader reader, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(String columnLabel, Reader reader, long length)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set,
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(int columnIndex, Reader reader, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(int columnIndex, Reader reader, long length)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object, which is the given number of characters long.
     * When a very large UNICODE value is input to a <code>LONGVARCHAR</code>
     * parameter, it may be more practical to send it via a
     * <code>java.io.Reader</code> object. The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @param length      the number of characters in the parameter data.
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(String columnLabel, Reader reader, long length) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(String columnLabel, Reader reader, long length)");
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * It is intended for use when
     * updating  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateNCharacterStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNCharacterStream(int columnIndex, Reader x)");
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The
     * driver does the necessary conversion from Java character format to
     * the national character set in the database.
     * It is intended for use when
     * updating  <code>NCHAR</code>,<code>NVARCHAR</code>
     * and <code>LONGNVARCHAR</code> columns.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateNCharacterStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing
     *                    the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code> or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNCharacterStream(String columnLabel, Reader reader) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateNCharacterStream(String columnLabel, Reader reader)");
    }

    /**
     * Updates the designated column with an ascii stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateAsciiStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(int columnIndex, InputStream x)");
    }

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateBinaryStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(int columnIndex, InputStream x)");
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateCharacterStream</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param x           the new column value
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(int columnIndex, Reader x)");
    }

    /**
     * Updates the designated column with an ascii stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateAsciiStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateAsciiStream(String columnLabel, InputStream x)");
    }

    /**
     * Updates the designated column with a binary stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateBinaryStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param x           the new column value
     * @throws SQLException                    if the columnLabel is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBinaryStream(String columnLabel, InputStream x)");
    }

    /**
     * Updates the designated column with a character stream value.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateCharacterStream</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      the <code>java.io.Reader</code> object containing
     *                    the new column value
     * @throws SQLException                    if the columnLabel is not valid; if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateCharacterStream(String columnLabel, Reader reader) throws
            SQLException {
        throw new SQLFeatureNotSupportedException("void updateCharacterStream(String columnLabel, Reader reader)");
    }

    /**
     * Updates the designated column using the given input stream. The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateBlob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param inputStream An object that contains the data to set the parameter
     *                    value to.
     * @throws SQLException                    if the columnIndex is not valid; if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(int columnIndex, InputStream inputStream)");
    }

    /**
     * Updates the designated column using the given input stream. The data will be read from the stream
     * as needed until end-of-stream is reached.
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateBlob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param inputStream An object that contains the data to set the parameter
     *                    value to.
     * @throws SQLException                    if the columnLabel is not valid; if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateBlob(String columnLabel, InputStream inputStream)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateClob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(int columnIndex, Reader reader)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateClob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws SQLException                    if the columnLabel is not valid; if a database access error occurs;
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     *                                         or this method is called on a closed result set
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateClob(String columnLabel, Reader reader)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * <p>
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateNClob</code> which takes a length parameter.
     *
     * @param columnIndex the first column is 1, the second 2, ...
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws SQLException                    if the columnIndex is not valid;
     *                                         if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set,
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(int columnIndex, Reader reader)");
    }

    /**
     * Updates the designated column using the given <code>Reader</code>
     * object.
     * The data will be read from the stream
     * as needed until end-of-stream is reached.  The JDBC driver will
     * do any necessary conversion from UNICODE to the database char format.
     *
     * <p>
     * The updater methods are used to update column values in the
     * current row or the insert row.  The updater methods do not
     * update the underlying database; instead the <code>updateRow</code> or
     * <code>insertRow</code> methods are called to update the database.
     *
     * <P><B>Note:</B> Consult your JDBC driver documentation to determine if
     * it might be more efficient to use a version of
     * <code>updateNClob</code> which takes a length parameter.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.  If the SQL AS clause was not specified, then the label is the name of the column
     * @param reader      An object that contains the data to set the parameter value to.
     * @throws SQLException                    if the columnLabel is not valid; if the driver does not support national
     *                                         character sets;  if the driver can detect that a data conversion
     *                                         error could occur; this method is called on a closed result set;
     *                                         if a database access error occurs or
     *                                         the result set concurrency is <code>CONCUR_READ_ONLY</code>
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.6
     */
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("void updateNClob(String columnLabel, Reader reader)");
    }

    /**
     * <p>Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object and will convert from the
     * SQL type of the column to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported  or null is specified for the type, a
     * <code>SQLException</code> is thrown.
     * <p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param columnIndex the first column is 1, the second is 2, ...
     * @param type        Class representing the Java data type to convert the designated
     *                    column to.
     * @return an instance of {@code type} holding the column value
     * @throws SQLException                    if conversion is not supported, type is null or
     *                                         another error occurs. The getCause() method of the
     *                                         exception may provide a more detailed exception, for example, if
     *                                         a conversion error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.7
     */
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("<T> T getObject(int columnIndex, Class<T> type)");
    }

    /**
     * <p>Retrieves the value of the designated column in the current row
     * of this <code>ResultSet</code> object and will convert from the
     * SQL type of the column to the requested Java data type, if the
     * conversion is supported. If the conversion is not
     * supported  or null is specified for the type, a
     * <code>SQLException</code> is thrown.
     * <p>
     * At a minimum, an implementation must support the conversions defined in
     * Appendix B, Table B-3 and conversion of appropriate user defined SQL
     * types to a Java type which implements {@code SQLData}, or {@code Struct}.
     * Additional conversions may be supported and are vendor defined.
     *
     * @param columnLabel the label for the column specified with the SQL AS clause.
     *                    If the SQL AS clause was not specified, then the label is the name
     *                    of the column
     * @param type        Class representing the Java data type to convert the designated
     *                    column to.
     * @return an instance of {@code type} holding the column value
     * @throws SQLException                    if conversion is not supported, type is null or
     *                                         another error occurs. The getCause() method of the
     *                                         exception may provide a more detailed exception, for example, if
     *                                         a conversion error occurs
     * @throws SQLFeatureNotSupportedException if the JDBC driver does not support
     *                                         this method
     * @since 1.7
     */
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException("<T> T getObject(String columnLabel, Class<T> type)");
    }

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy.
     * <p>
     * If the receiver implements the interface then the result is the receiver
     * or a proxy for the receiver. If the receiver is a wrapper
     * and the wrapped object implements the interface then the result is the
     * wrapped object or a proxy for the wrapped object. Otherwise return the
     * the result of calling <code>unwrap</code> recursively on the wrapped object
     * or a proxy for that result. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws SQLException If no object found that implements the interface
     * @since 1.6
     */
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("<T> T unwrap(Class<T> iface)");
    }

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param iface a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws SQLException if an error occurs while determining whether this is a wrapper
     *                      for an object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException("boolean isWrapperFor(Class<?> iface)");
    }
}
