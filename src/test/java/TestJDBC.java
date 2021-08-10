import java.sql.*;

public class TestJDBC {


    public static void main(String[] args) throws Exception {
        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        String url = "jdbc:pega:http://localhost:8080/prweb/PRRestService/JDBCAPI/v1/PegaJDBC/";
        try (


                Connection conn = DriverManager.getConnection(url);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("select pyLabel from pr_myco_myapp_data_test")) {

            while (rs.next())
                System.out.println(rs.getString(1));
        } catch (Exception e) {
            System.err.println(e.toString());
        }
    }
}
