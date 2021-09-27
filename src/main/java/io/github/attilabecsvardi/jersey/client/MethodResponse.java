package io.github.attilabecsvardi.jersey.client;

import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

/**
 * @author attilabecsvardi
 */
public class MethodResponse {

    private String returnValue = null;

    public SQLWarning getSqlWarning() {
        return sqlWarning;
    }

    public void setSqlWarning(SQLWarning sqlWarning) {
        this.sqlWarning = sqlWarning;
    }

    private SQLWarning sqlWarning = null;

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    private String error = null;

    public ArrayList<ColumnMetaData> getColumnList() {
        return columnList;
    }

    public void setColumnList(ArrayList<ColumnMetaData> columnList) {
        this.columnList = columnList;
    }

    private ArrayList<ColumnMetaData> columnList = null;


    public ArrayList getRecordList() {
        return recordList;
    }

    public void setRecordList(ArrayList recordList) {
        this.recordList = recordList;
    }


    //private ArrayList<ArrayList<String>> rsList = null;
    private ArrayList recordList = null;
    //private List record = null;


    public MethodResponse() {
    }

    public String getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(String returnValue) {
        this.returnValue = returnValue;
    }
}
