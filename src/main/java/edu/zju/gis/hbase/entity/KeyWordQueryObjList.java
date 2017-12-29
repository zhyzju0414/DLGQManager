package edu.zju.gis.hbase.entity;

import java.util.ArrayList;
import java.util.List;

import edu.zju.gis.hbase.tool.CategoryManager;
import edu.zju.gis.hbase.tool.Utils;

public class KeyWordQueryObjList {



    public KeyWordQueryObjList() {
        super();
        OBJLIST = new ArrayList<KeyWordQueryObj>();
    }

    /*
     * 标题舍去分类关键字
     */
    public void Add(String rOWCODE, String cAPTION){
        if(cAPTION!=null&&cAPTION.length()>0){
            cAPTION = cAPTION.split(" ")[0];
        }
        OBJLIST.add(new KeyWordQueryObj(rOWCODE, cAPTION));
    }

    @Override
    public String toString() {
        return "KeyWordQueryObjList [OBJLIST=" + OBJLIST + "]";
    }

    public List<KeyWordQueryObj> OBJLIST;

    public class KeyWordQueryObj{
        public String ROWCODE;
        public String CAPTION;
        public String CATEGORYCODE;
        public String PARENTCATEGORY;

        public KeyWordQueryObj(String rOWCODE, String cAPTION) {
            super();
            ROWCODE = rOWCODE;
            CAPTION = cAPTION;
            CATEGORYCODE = Utils.GetCategoryCode(ROWCODE);
            PARENTCATEGORY = CategoryManager.GetParentLevelCode(CATEGORYCODE);
        }

        @Override
        public String toString() {
            return "KeyWordQueryObj [ROWCODE=" + ROWCODE + ", CAPTION="
                    + CAPTION + ", CATEGORYCODE=" + CATEGORYCODE
                    + ", PARENTCATEGORY=" + PARENTCATEGORY + "]";
        }



    }
}
