package io.github.attilabecsvardi.jersey.client;

/**
 * @author attilabecsvardi
 */
public class MethodResponse {

    private String returnValue = null;

    public MethodResponse() {
    }

    public String getReturnValue() {
        return returnValue;
    }

    public void setReturnValue(String returnValue) {
        this.returnValue = returnValue;
    }
}
