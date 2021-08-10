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

    public JDBCMethod() {
    }

    public JDBCMethod(String methodName, ArrayList<Parameter> paramList) {
        this.methodName = methodName;
        this.paramList = paramList;
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
}
