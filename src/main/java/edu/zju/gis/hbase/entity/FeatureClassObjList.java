package edu.zju.gis.hbase.entity;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "FeatureClassObjList")
public class FeatureClassObjList {
    private List<FeatureClassObj> featureClassObjList;


    public FeatureClassObjList() {
        super();
        featureClassObjList = new ArrayList<FeatureClassObj>();
    }

    public List<FeatureClassObj> getFeatureClassObjList() {
        return featureClassObjList;
    }

    public void setFeatureClassObjList(List<FeatureClassObj> featureClassObjList) {
        this.featureClassObjList = featureClassObjList;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<featureClassObjList.size();i++){
            sb.append(featureClassObjList.get(i)+"\n");
        }
        return sb.toString();
    }

    public void AddFeatureClassObj(FeatureClassObj obj){
        featureClassObjList.add(obj);
    }
}
