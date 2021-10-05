package io.github.attilabecsvardi.jersey.client;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

/**
 * @author attilabecsvardi
 */
public class MethodResponse {

    private String returnValue = null;
    private SQLWarning sqlWarning = null;
    private String error = null;
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

    public String getError() {
        return error;
    }

    public void setError(String error) {
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
