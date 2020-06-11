package com.cisco.wap.jdbc;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;

public class VoldemortDatabaseMetaData extends AvaticaDatabaseMetaData {
    protected VoldemortDatabaseMetaData(AvaticaConnection connection) {
        super(connection);
    }
}
