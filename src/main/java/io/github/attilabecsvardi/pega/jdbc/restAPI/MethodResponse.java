package io.github.attilabecsvardi.pega.jdbc.restAPI;

import java.sql.SQLWarning;
import java.util.ArrayList;

/**
 * @author attilabecsvardi
 */
public class MethodResponse {

    private String returnValue = null;
    private SQLWarning sqlWarning = null;
    private JDBCError error = null;
    private ArrayList<ColumnMetaData> columnList = null;
    private ArrayList recordList = null;

    public MethodResponse() {
    }

    public String getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(String returnValue) {
        this.returnValue = returnValue;
    }

    public SQLWarning getSqlWarning() {
        return sqlWarning;
    }

    public void setSqlWarning(SQLWarning sqlWarning) {
        this.sqlWarning = sqlWarning;
    }

    public JDBCError getError() {
        return error;
    }

    public void setError(JDBCError error) {
        this.error = error;
    }

    public ArrayList<ColumnMetaData> getColumnList() {
        return columnList;
    }

    public void setColumnList(ArrayList<ColumnMetaData> columnList) {
        this.columnList = columnList;
    }

    public ArrayList getRecordList() {
        return recordList;
    }

    public void setRecordList(ArrayList recordList) {
        this.recordList = recordList;
    }


}
