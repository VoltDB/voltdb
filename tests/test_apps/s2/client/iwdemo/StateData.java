package iwdemo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StateData {
    Map<String, Long>               shapeidMap  = new HashMap<String, Long>();
    Map<Long,   StateAttributeBean> idMap       = new HashMap<Long, StateAttributeBean>();
    public boolean addState(StateAttributeBean bean) {
        Long oldsid = shapeidMap.get(bean.getSHAPEID());
        StateAttributeBean oldBean = idMap.get(bean.getSTATEID());
        if (oldsid == null) {
            shapeidMap.put(bean.getSHAPEID(), bean.getSTATEID());
        } else if (oldBean != null && oldsid != oldBean.getSTATEID()) {
            System.out.printf("Two state ids (%d and %d) for shape id %s\n",
                              oldsid, oldBean.getSTATEID(),
                              bean.getSHAPEID());
        }
        if (oldBean == null) {
            idMap.put(bean.getSTATEID(), bean);
            return true;
        }
        return false;
    }

    public Long getStateId(String shapeId) {
        return shapeidMap.get(shapeId);
    }

    public StateAttributeBean getByStateID(Long stateid) {
        return idMap.get(stateid);
    }

    public StateAttributeBean getByShapeID(String shapeid) {
        if (shapeid == null) {
            return null;
        }
        Long stateid = shapeidMap.get(shapeid);
        if (stateid == null) {
            return null;
        }
        return idMap.get(stateid);
    }

    public Set<Map.Entry<Long, StateAttributeBean>> getIDEntrySet() {
        return idMap.entrySet();
    }
}
