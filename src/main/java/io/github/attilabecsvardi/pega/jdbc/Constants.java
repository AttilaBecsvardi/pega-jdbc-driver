package io.github.attilabecsvardi.pega.jdbc;

import java.sql.ResultSet;

/**
 * Constants are fixed values that are used in the whole code.
 */
class Constants {
    public static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    public static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    public static final int DEFAULT_RESULT_SET_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;


    /**
     * The database URL prefix of this database.
     */
    static final String START_URL = "jdbc:pega:";

    static final int VERSION_MAJOR = 0;

    static final int VERSION_MINOR = 0;
}
