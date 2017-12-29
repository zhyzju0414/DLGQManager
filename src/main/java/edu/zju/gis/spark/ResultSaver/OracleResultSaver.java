package edu.zju.gis.spark.ResultSaver;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Created by dlgq on 2017/8/16.
 */
public class OracleResultSaver implements IResultSaver {
    public boolean SaveResult(String key, String value)
    {
        if (OracleOperator.TaskExist(key)) {
            try
            {
                Blob blob = OracleOperator.conn.createBlob();
                OutputStream out = blob.setBinaryStream(1L);
                out.write(value.getBytes(), 0, value.getBytes().length);
                out.flush();

                PreparedStatement pstmt = OracleOperator.conn.prepareStatement("update T_TASK set RESULT = ? where TASKID = '" + key + "'");

                pstmt.setBlob(1, blob);
                pstmt.executeUpdate();

                pstmt.close();
                out.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public static void main(String[] args) throws Exception
    {
        OracleResultSaver saver = new OracleResultSaver();

        saver.SaveResult("asdasfdafasdafsdsaf", "mknzba0kjkjkjkjkj123343lj");
    }
}
