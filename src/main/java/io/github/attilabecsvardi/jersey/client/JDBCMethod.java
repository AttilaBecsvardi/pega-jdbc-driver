package io.github.attilabecsvardi.jersey.client;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Produces;

import java.io.Serializable;
import java.util.ArrayList;

/**
 * @author attilabecsvardi
 */
@Consumes
@Produces
public class JDBCMethod implements Serializable {

    private String methodName;
    private ArrayList<Parameter> paramList;
    private String retInstanceID;

    public JDBCMethod() {
    }

    public JDBCMethod(String methodName, ArrayList<Parameter> paramList) {
        this.methodName = methodName;
        this.paramList = paramList;
    }

    public JDBCMethod(String methodName, ArrayList<Parameter> paramList, String retInstanceID) {
        this.methodName = methodName;
        this.paramList = paramList;
        this.retInstanceID = retInstanceID;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public ArrayList<Parameter> getParamList() {
        return paramList;
    }

    public String getRetInstanceID() {
        return retInstanceID;
    }

    public void setRetInstanceID(String retInstanceID) {
        this.retInstanceID = retInstanceID;
    }
}
