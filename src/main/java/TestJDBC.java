import com.fasterxml.jackson.core.io.CharacterEscapes;
import io.github.attilabecsvardi.pega.jdbc.PegaDriver;

import java.io.IOException;
import java.io.StringReader;
import java.sql.*;
import java.util.*;

public class TestJDBC {


    public static void main(String[] args) throws Exception {

        com.fasterxml.jackson.core.JsonFactory jsonFactory = new com.fasterxml.jackson.core.JsonFactory();
        java.io.StringWriter sw = new java.io.StringWriter();

        try {
            com.fasterxml.jackson.core.JsonGenerator jg = jsonFactory.createGenerator(sw);

            String[] sa = new String[]{"aaa", "fc"};
            jg.writeStartObject();
            jg.writeFieldName("ass");
            String s = "sada\nsad";
            System.out.println(s);
            jg.writeString(s);
            jg.writeEndObject();
            jg.flush();
            System.out.println(sw.toString());

        /*    jg.writeArray(sa,0,2);

            jg.writeFieldName("assdddddd");
            jg.writeStartArray();
            jg.writeStartObject();

            for (int i = 0; i < sa.length; i++) {
                jg.writeFieldName(""+i);
                if (i==0) {
                jg.writeString(sa[i]);}
                else {jg.writeNull();}
                //jg.writeStringField(String.valueOf(i),sa[i]);
            }
            jg.writeEndObject();

            jg.writeEndArray();

            jg.writeEndObject();

            jg.flush();
            System.out.println(sw.toString());*/

        } catch (IOException e) {
            e.printStackTrace();
        }


        String sss = "{ApplicationName=PostgreSQL JDBC Driver,dddddddddddd=fee}";
        Properties p = new Properties();
        p.load(new StringReader(sss));
        p.setProperty("sssssss", "sded");

        Enumeration<String> enums = (Enumeration<String>) p.propertyNames();
        while (enums.hasMoreElements()) {
            String key = enums.nextElement();
            String value = p.getProperty(key);
            System.out.println(key + " : " + value);
        }

        System.out.println(p.toString());

        //Class.forName("io.github.attilabecsvardi.pega.jdbc.PegaDriver");
        Driver d = new PegaDriver();
        DriverManager.registerDriver(d);

        String url = "jdbc:pega:http://localhost:8080/prweb/PRRestService/JDBCAPI/v1/PegaJDBC/";
        //String url = "jdbc:pega:https://pega-marketing-dev.bkt.com.al/prweb/PRRestService/JDBCAPI/v1/PegaJDBC/";

        Properties myInfo = new Properties();
        myInfo.setProperty("user", "test");
        myInfo.setProperty("password", "rules");
        //myInfo.setProperty("user", "dbattila.becsvardi@stc");
        //myInfo.setProperty("password", "PegaRULES22_");
        myInfo.setProperty("dbName", "PegaRULES");


            try (Connection conn = DriverManager.getConnection(url, myInfo)) {

                SQLWarning w = conn.getWarnings();

                CallableStatement cStmt = conn.prepareCall("{ ? = call upper( ? ) }");
                cStmt.registerOutParameter(1, Types.VARCHAR);
                cStmt.setString(2, "lowercase to uppercase");
                cStmt.execute();
                String upperCased = cStmt.getString(1);
                cStmt.close();


                String sqlp = "select pyLabel,pyGUID from pr_myco_myapp_data_test where pylabel=?";
                sqlp = "select * from pg_catalog.pg_settings";
                sqlp = "SELECT n.oid,n.*,d.description FROM pg_catalog.pg_namespace n\n" +
                        "LEFT OUTER JOIN pg_catalog.pg_description d ON d.objoid=n.oid AND d.objsubid=0 AND d.classoid='pg_namespace'::regclass\n" +
                        " WHERE nspname='data' ORDER BY nspname";
                sqlp = " SELECT c.relname,a.*,pg_catalog.pg_get_expr(ad.adbin, ad.adrelid, true) as def_value,dsc.description FROM pg_catalog.pg_attribute a INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid) LEFT OUTER JOIN pg_catalog.pg_attrdef ad ON (a.attrelid=ad.adrelid AND a.attnum = ad.adnum) LEFT OUTER JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid) WHERE NOT a.attisdropped AND c.oid=? ORDER BY a.attnum \n";

                //sqlp="EXPLAIN (FORMAT XML) SELECT sa.* FROM pg_catalog.pg_stat_activity sa";
            /*    sqlp="SELECT  O.*," +
                        "t.TABLE_TYPE_OWNER,t.TABLE_TYPE,t.TABLESPACE_NAME,t.PARTITIONED,t.IOT_TYPE,t.IOT_NAME,t.TEMPORARY,t.SECONDARY,t.NESTED,t.NUM_ROWS" +
                        " FROM ALL_OBJECTS O " +
                        ", ALL_ALL_TABLES t WHERE t.OWNER(+) = O.OWNER AND t.TABLE_NAME(+) = o.OBJECT_NAME " +
                        "AND O.OWNER='PEGA_DATA_MARKETING' AND O.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')";
           */
                PreparedStatement preparedStatement = conn.prepareStatement(sqlp);

                preparedStatement = conn.prepareStatement(sqlp);

                //preparedStatement.setLong(1, 495249);
                // preparedStatement.setString(1,"asdasf");
                //preparedStatement.setFetchSize(100);
                preparedStatement.execute();
                ResultSet rsp = null;
                rsp = preparedStatement.getResultSet();
                //rsp = preparedStatement.executeQuery();
            ResultSetMetaData rspMetaData = rsp.getMetaData();
            //int r =preparedStatement.executeUpdate();

            while (rsp.next())
                for (int i = 1; i <= rspMetaData.getColumnCount(); i++) {
                    System.out.println("CName: " + rspMetaData.getColumnName(i) + ", CValue: " + rsp.getInt(i));
                }

                preparedStatement.close();

            String sql = "select pyLabel,pyGUID from pr_myco_myapp_data_test";
            Statement stmt = conn.createStatement();
            //stmt.setMaxRows(200);


            String query = "select 'a' as b, 'b' as b";
                query = "select * from dual";
            ResultSet myrs4 = stmt.executeQuery(query);

            while (myrs4.next()) {
                System.out.println(myrs4.getString(1));
            }


            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("getSearchStringEscape: " + meta.getSearchStringEscape());
            //ResultSet rsMeta = meta.getSchemas();
            //rsMeta = meta.getTables("postgres", "pegadata", "%", null);
            ResultSet rsMeta = meta.getColumns("postgres", "pegadata", "pr_ltf_se_int_file_memattrs_er", "%");
            ResultSetMetaData rsMetaData = rsMeta.getMetaData();
            //System.out.println(rsMetaData.toString());

            while (rsMeta.next()) {
                for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                    System.out.println("CName: " + rsMetaData.getColumnName(i) + ", CValue: " + rsMeta.getString(i));
                }
                //System.out.println(rsMeta.getString(1)+" "+rsMeta.getString("table_schem"));
                //System.out.println(rsMeta.getString(2)+" "+rsMeta.getString("table_catalog"));
            }

            rsMeta.close();
            boolean re = stmt.execute(sql);

            if (re) System.out.println("execute");

            int c = stmt.getUpdateCount();
            System.out.println("count " + c);

            ResultSet rs = stmt.getResultSet();
            while (rs.next())
                System.out.println(rs.getString(1));
            stmt.close();
            conn.clearWarnings();

            System.out.println("catalog " + conn.getCatalog());
            System.out.println("schema " + conn.getSchema());


        } catch (Exception e) {
            System.err.println(e);
        }


    }
}
