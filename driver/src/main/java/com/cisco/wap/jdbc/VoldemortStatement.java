package com.cisco.wap.jdbc;

import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta;

public class VoldemortStatement extends AvaticaStatement {
    protected VoldemortStatement(VoldemortConnection connection,
                                 Meta.StatementHandle handle,
                                 int resultSetType,
                                 int resultSetConcurrency,
                                 int resultSetHoldability) {
        super(connection, handle, resultSetType, resultSetConcurrency, resultSetHoldability);
    }
}
