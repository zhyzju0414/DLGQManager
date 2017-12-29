package edu.zju.gis.hbase.entity;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.hbase.util.Bytes;

import edu.zju.gis.hbase.tool.QueryCondition;

@XmlRootElement(name = "QueryParameter")
public class QueryParameter {
    @Override
    public String toString() {
        return "QueryParameter [regionCode=" + regionCode + ", geoWKT="
                + geoWKT + ", categoryCode=" + categoryCode + ", startIndex="
                + startIndex + ", length=" + length + "]";
    }

    private static final byte[] POSTFIX = new byte[] { 0x00 };

    public String regionCode;

    public String geoWKT;

    public String categoryCode;

    public String startIndex = "-1";

    public String length;

    public String keyWords;     //11.30

    public boolean statisticOnly = false;  //只进行统计

    public boolean isStatisticOnly() {
        return statisticOnly;
    }

    public void setStatisticOnly(boolean statisticOnly) {
        this.statisticOnly = statisticOnly;
    }

    public String getGeoWKT() {
        return geoWKT;
    }

    public void setGeoWKT(String geoWKT) {
        this.geoWKT = geoWKT;
    }

    public String getStartIndex() {
        return startIndex;
    }

    public void setStartIndex(String startIndex) {
        this.startIndex = startIndex;
    }

    public String getLength() {
        return length;
    }

    public void setLength(String length) {
        this.length = length;
    }

    public String getRegionCode() {
        return regionCode;
    }

    public void setRegionCode(String regionCode) {
        this.regionCode = regionCode;
    }

    public String getCategoryCode() {
        return categoryCode;
    }

    public void setCategoryCode(String categoryCode) {
        this.categoryCode = categoryCode;
    }

    public byte[] GetStartRow(){
        if(startIndex.equals("-1")){
            return null;
        }
        return Bytes.add(Bytes.toBytes(startIndex), POSTFIX);
    }

    /*
     * 获取在分页查询中前一次记录列表的最后一条记录
     */
    public byte[] GetPreLastRow(){
        if(startIndex.equals("-1")){
            return null;
        }
        return Bytes.toBytes(startIndex);
    }

    /*
     * 判断是否分页查询
     */
    public boolean isPageQuery(){
        return !(startIndex==null||length==null);
    }

    /*
     * 判断是否需要进行空间查询
     */
    public boolean isSpatialQuery(){
        return !(geoWKT==null);
    }



    public QueryCondition GetQueryCondition() throws Exception{
        QueryCondition qc = new QueryCondition();
        if(isSpatialQuery()){
            qc.InitializeSpatialQueryParameter(geoWKT, categoryCode);
        }else
            qc.InitializePropertyQueryParameter(regionCode, categoryCode);
        return qc;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((categoryCode == null) ? 0 : categoryCode.hashCode());
        result = prime * result + ((geoWKT == null) ? 0 : geoWKT.hashCode());
        //	result = prime * result + ((length == null) ? 0 : length.hashCode());
        result = prime * result
                + ((regionCode == null) ? 0 : regionCode.hashCode());
//		result = prime * result
//				+ ((startIndex == null) ? 0 : startIndex.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryParameter other = (QueryParameter) obj;
        if (categoryCode == null) {
            if (other.categoryCode != null)
                return false;
        } else if (!categoryCode.equals(other.categoryCode))
            return false;
        if (geoWKT == null) {
            if (other.geoWKT != null)
                return false;
        } else if (!geoWKT.equals(other.geoWKT))
            return false;
        if (length == null) {
            if (other.length != null)
                return false;
        } else if (!length.equals(other.length))
            return false;
        if (regionCode == null) {
            if (other.regionCode != null)
                return false;
        } else if (!regionCode.equals(other.regionCode))
            return false;
        if (startIndex == null) {
            if (other.startIndex != null)
                return false;
        } else if (!startIndex.equals(other.startIndex))
            return false;
        return true;
    }

}
