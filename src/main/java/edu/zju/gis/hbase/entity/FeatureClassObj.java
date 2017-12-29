package edu.zju.gis.hbase.entity;

import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.tool.Utils;
import edu.zju.gis.hbase.tool.Utils.StatisticIndexEnum;
@XmlRootElement(name = "FeatureClassObj")
public class FeatureClassObj {
    public String ID;

    public String GEOMETRYWKT;

    public String CAPTION;

    public Map<String,String> PROPERTY;

    public String POSITION;

    public String FEATURETYPE;

    public double AREA;  //面积

    public double LENGTH;  //长度


    private String getFEATURETYPE() {
        return FEATURETYPE;
    }

    private void setFEATURETYPE(String fEATURETYPE) {
        FEATURETYPE = fEATURETYPE;
    }

    private String getID() {
        return ID;
    }

    private void setID(String iD) {
        ID = iD;
    }

    private String getGEOMETRYWKT() {
        return GEOMETRYWKT;
    }

    private void setGEOMETRYWKT(String gEOMETRYWKT) {
        GEOMETRYWKT = gEOMETRYWKT;
    }

    private String getCAPTION() {
        return CAPTION;
    }

    private void setCAPTION(String cAPTION) {
        CAPTION = cAPTION;
    }

    private Map<String, String> getPROPERTY() {
        return PROPERTY;
    }

    private void setPROPERTY(Map<String, String> pROPERTY) {
        PROPERTY = pROPERTY;
    }

    private String getPOSITION() {
        return POSITION;
    }

    private void setPOSITION(String pOSITION) {
        POSITION = pOSITION;
    }

    private double getAREA() {
        return AREA;
    }

    private void setAREA(double aREA) {
        AREA = aREA;
    }

    private double getLENGTH() {
        return LENGTH;
    }

    private void setLENGTH(double lENGTH) {
        LENGTH = lENGTH;
    }

    public static FeatureClassObj ConstructFeatureClassObj(Result result){
        FeatureClassObj obj = new FeatureClassObj();
        obj.setID(Bytes.toString(result.getRow()));
        obj.setCAPTION(Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.CAPTION_QUALIFIER)));
        byte[] geowkt = result.getValue(Utils.COLUMNFAMILY_GEOMETRY, Utils.GEOMETRY_QUALIFIER) ;
        if(geowkt!=null){
            obj.setGEOMETRYWKT(Bytes.toString(geowkt));
        }
        obj.setPOSITION(Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.POSITION_QUALIFIER)));
        String property = Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.PROPERTY_QUALIFIER));
        obj.setPROPERTY(Utils.resolveProperty(property));
        obj.setFEATURETYPE(Bytes.toString(result.getValue(Utils.COLUMNFAMILY_PROPERTY, Utils.FEATURETYPE_QUALIFIER)));
        String areaStr = Bytes.toString(result.getValue(Utils.COLUMNFAMILY_STATINDEX,Bytes.toBytes(StatisticIndexEnum.AREA.toString())));
        if(areaStr!=null&&areaStr.length()>0){
            obj.setAREA(Double.parseDouble(areaStr));
        }
        String lengthStr =  Bytes.toString(result.getValue(Utils.COLUMNFAMILY_STATINDEX,Bytes.toBytes(StatisticIndexEnum.LENGTH.toString())));
        if(lengthStr!=null&&lengthStr.length()>0){
            obj.setLENGTH(Double.parseDouble(lengthStr));
        }
        return obj;
    }



    String GetPropertyStr(){
        StringBuffer sb = new StringBuffer();
        for (Map.Entry<String, String> entry : PROPERTY.entrySet()) {
            sb.append("Key = " + entry.getKey() + ", Value = " + entry.getValue());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "FeatureClassObj [ID=" + ID + ", GEOMETRYWKT=" + GEOMETRYWKT
                + ", CAPTION=" + CAPTION + ", PROPERTY=" + GetPropertyStr()
                + ", POSITION=" + POSITION + "]"+" "+LENGTH+" "+AREA;
    }


}
