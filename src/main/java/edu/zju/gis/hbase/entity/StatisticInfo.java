package edu.zju.gis.hbase.entity;


public class StatisticInfo {


    public StatisticInfo() {
        super();
        this.COUNT = 0;
        this.AREA = 0;
        this.LENGTH = 0;
        this.AREAPERCENT = 0;
        this.COUNTPERCENT = 0;
        this.LENGTHPERCENT = 0;
    }

    public StatisticInfo(long count,double area,double length) {
        super();
        this.COUNT = count;
        this.AREA = area;
        this.LENGTH = length;
        this.AREAPERCENT = 0;
        this.COUNTPERCENT = 0;
        this.LENGTHPERCENT = 0;
    }

    public long COUNT;

    public double AREA;

    public double LENGTH;

    public double AREAPERCENT;   //面积占比  1124

    public double COUNTPERCENT;  //个数占比  1124

    public double LENGTHPERCENT;  //长度占比  1124


    @Override
    public String toString() {
        return "StatisticInfo [COUNT=" + COUNT + ", AREA=" + AREA
                + ", LENGTH=" + LENGTH + ", AREAPERCENT=" + AREAPERCENT
                + ", COUNTPERCENT=" + COUNTPERCENT + ", LENGTHPERCENT="
                + LENGTHPERCENT + "]";
    }

    private long getCOUNT() {
        return COUNT;
    }

    private double getAREAPERCENT() {
        return AREAPERCENT;
    }

    private void setAREAPERCENT(double aREAPERCENT) {
        AREAPERCENT = aREAPERCENT;
    }

    private double getCOUNTPERCENT() {
        return COUNTPERCENT;
    }

    private void setCOUNTPERCENT(double cOUNTPERCENT) {
        COUNTPERCENT = cOUNTPERCENT;
    }

    private double getLENGTHPERCENT() {
        return LENGTHPERCENT;
    }

    private void setLENGTHPERCENT(double lENGTHPERCENT) {
        LENGTHPERCENT = lENGTHPERCENT;
    }

    private void setCOUNT(long cOUNT) {
        COUNT = cOUNT;
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
}



