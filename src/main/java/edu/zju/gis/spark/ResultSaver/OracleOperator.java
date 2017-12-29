package edu.zju.gis.spark.ResultSaver;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by dlgq on 2017/8/16.
 */
public class OracleOperator {
    public static Properties prop;
    public static boolean connected = false;
    public static Connection conn;
    public static PreparedStatement pre;

    static
    {
//        prop = new Properties();
//        String propath = "E:\\zhy\\dlgq.properties";
//        OracleOperator operator = new OracleOperator();
//        InputStream in = operator.getClass().getResourceAsStream(propath);
//        try {
//            prop.load(new InputStreamReader(in, "UTF-8"));
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public static Connection getOracleConnection() throws Exception
    {
        if (!connected) {
            String driver = "oracle.jdbc.driver.OracleDriver";
//            String url = "jdbc:oracle:thin:@" +
//                    prop.getProperty("oracleInstance");
//            String username = prop.getProperty("user");
//            String password = prop.getProperty("password");

            String url = "jdbc:oracle:thin:@" +"10.2.35.109:1521:zjorcl";
            String username = "DLGQ_MANAGER";
            String password = "DLGQ_2014";
            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            connected = true;
        }
        return conn;
    }

    public static Connection getOracleConnection(String oracleInstance, String user, String password) throws Exception {
        if (!connected) {
            String driver = "oracle.jdbc.driver.OracleDriver";
//            String url = "jdbc:oracle:thin:@" +
//                    oracleInstance;
            String url = "jdbc:oracle:thin:@" +"10.2.35.109:zjorcl";

            Class.forName(driver);
            conn = DriverManager.getConnection(url, user, password);
            connected = true;
        }
        return conn;
    }

    public static void Close()
    {
        if (conn != null)
            try {
                pre.close();
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
    }

    public static boolean TaskExist(String key)
    {
        try
        {
            String sql = "select * from T_TASK where TASKID='" + key + "'";
            conn = getOracleConnection();
            pre = conn.prepareStatement(sql);
            ResultSet result = pre.executeQuery();

            return result.next();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            System.out.println(e.toString());
        }

        return false;
    }

    public static synchronized void execute(String sql)
            throws SQLException
    {
        label127:
        try { pre = getOracleConnection().prepareStatement(sql);
            if (pre != null) {
                pre.executeUpdate(sql);
                if (pre != null) {
                    pre.close();

                    break label127;
                } } else {
                System.out.println("数据库操作执行失败：" + sql);
            }
        } catch (Exception e)
        {
            System.out.println("数据库操作执行失败：" + sql);
        }
        finally {
            if (pre != null)
            {
                pre.close();
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        TaskExist("asdasfdafasdafsdsaf");
    }
}
