package io.github.attilabecsvardi.pega.jdbc;

import io.github.attilabecsvardi.pega.jdbc.restAPI.JDBCError;
import io.github.attilabecsvardi.pega.jdbc.restAPI.JDBCMethod;
import io.github.attilabecsvardi.pega.jdbc.restAPI.MethodResponse;
import io.github.attilabecsvardi.pega.jdbc.restAPI.RestClient;

import javax.ws.rs.core.Response;
import java.sql.SQLException;

public class Utils {
    public static MethodResponse callRemoteMethod(RestClient client, String instanceType, String instanceID, JDBCMethod method) throws SQLException {
        try (Response response = client.invokeJDBCMethod(instanceType, instanceID, method)) {
            int status = response.getStatus();

            if (status != 200) {
                SQLException se;

                if (status == 400) {
                    MethodResponse mr = response.readEntity(MethodResponse.class);
                    if (mr != null) {
                        JDBCError error = mr.getError();
                        if (error != null) {
                            int errorCode;
                            try {
                                errorCode = Integer.parseInt(error.getErrorCode());
                            } catch (NumberFormatException ne) {
                                errorCode = -1;
                            }
                            se = new SQLException(error.getErrorMessage(), error.getSqlState(), errorCode);
                        } else {
                            se = new SQLException("Failed to call " + method.getMethodName(), new Exception("NULL error"));
                        }
                    } else {
                        se = new SQLException("Failed to call " + method.getMethodName(), new Exception("NULL response"));
                    }
                } else {
                    se = new SQLException("Failed to call " + method.getMethodName(), new Exception("Response Status: " + status));
                }
                throw se;
            }

            return response.readEntity(MethodResponse.class);
        }
    }
}
