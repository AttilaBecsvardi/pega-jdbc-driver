import java.sql.*;

public class TestJDBC {


    public static void main(String[] args) throws Exception {
        Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        String url = "jdbc:pega:http://localhost:8080/prweb/PRRestService/JDBCAPI/v1/PegaJDBC/";
        try (
                Connection conn = DriverManager.getConnection(url)
        ) {

            String sql = "select pyLabel from pr_myco_myapp_data_test";
            //Statement stmt = conn.createStatement();
            //stmt.setMaxRows(200);


            DatabaseMetaData meta = conn.getMetaData();
            ResultSet rsMeta = meta.getTables("postgres", "pegadata", "%", null);

            while (rsMeta.next())
                System.out.println(rsMeta.getString(3));
/*
            boolean r = stmt.execute(sql);

            if (r) System.out.println("execute");

            int c = stmt.getUpdateCount();
            System.out.println("count " + c);

            ResultSet rs = stmt.getResultSet();
            while (rs.next())
                System.out.println(rs.getString(1));
            stmt.close();
            conn.clearWarnings();

            System.out.println("catalog " + conn.getCatalog());
            System.out.println("schema " + conn.getSchema());
*/

        } catch (Exception e) {
            System.err.println(e);
        }
    }
}
