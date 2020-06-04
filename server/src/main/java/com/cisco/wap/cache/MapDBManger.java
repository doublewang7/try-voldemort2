package com.cisco.wap.cache;

import com.google.common.collect.Maps;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

import java.util.Map;
import java.util.Objects;

public class MapDBManger {
    private static Map<String, HTreeMap> maps = Maps.newConcurrentMap();

    public synchronized static HTreeMap getInstance(String name) {
        HTreeMap map = maps.get(name);
        if(Objects.nonNull(map)) {
            return map;
        }
        //TODO: current mapdb is simple example
        DB db = DBMaker.memoryDB().make();
        map = db.hashMap(name).createOrOpen();
        maps.put(name, map);
        return map;
    }
}
