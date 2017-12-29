package edu.zju.gis.hbase.entity;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.tool.Utils;

@XmlRootElement(name = "Position")
public class Position {
    public String ROWCODE;

    private String getCAPTION() {
        return CAPTION;
    }

    private void setCAPTION(String cAPTION) {
        CAPTION = cAPTION;
    }

    public String POSITION;

    public String CAPTION;

    private String getRowCode() {
        return ROWCODE;
    }

    private void setRowCode(String rowCode) {
        this.ROWCODE = rowCode;
    }

    @Override
    public String toString() {
        return "Position [ROWCODE=" + ROWCODE + ", POSITION=" + POSITION
                + ", CAPTION=" + CAPTION + "]";
    }

    private String getPosition() {
        return POSITION;
    }



    private void setPosition(String position) {
        this.POSITION = position;
    }

    public static Position ConstructPosition(Result result){
        Position position = new Position();
        position.setRowCode(Bytes.toString(result.getRow()));
        position.setPosition(Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.POSITION_QUALIFIER)));
        position.setCAPTION(Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.CAPTION_QUALIFIER)));
        return position;
    }
}
