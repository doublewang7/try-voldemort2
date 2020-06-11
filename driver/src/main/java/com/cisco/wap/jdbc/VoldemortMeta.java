package com.cisco.wap.jdbc;

import org.apache.calcite.avatica.*;
import org.apache.calcite.avatica.remote.TypedValue;

import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class VoldemortMeta extends MetaImpl {


    public VoldemortMeta(VoldemortConnection connection) {
        super(connection);
    }

    @Override
    public ExecuteResult execute(StatementHandle h, List<TypedValue> parameterValues, long maxRowCount) throws NoSuchStatementException {
        return null;
    }

    private VoldemortConnection connection() {
        return (VoldemortConnection) connection;
    }

    @Override
    public ExecuteResult prepareAndExecute(StatementHandle handle,
                                           String sql,
                                           long maxRowCount,
                                           PrepareCallback callback) throws NoSuchStatementException {
        try {
            synchronized (callback.getMonitor()) {
                callback.clear();
                handle.signature = connection().mockPreparedSignature(sql);
                callback.assign(handle.signature, null, -1);
            }
            callback.execute();
            final MetaResultSet metaResultSet = MetaResultSet.create(handle.connectionId,
                    handle.id, false, handle.signature, null);
            return new ExecuteResult(Collections.singletonList(metaResultSet));
        } catch (SQLException e) {
            throw new NoSuchStatementException(handle);
        }
    }


    // do nothing, ignore all DWL
    @Override
    public void closeStatement(StatementHandle h) {

    }

    @Override
    public StatementHandle prepare(ConnectionHandle ch, String sql, long maxRowCount) {
        return null;
    }

    @Override
    public Frame fetch(StatementHandle h, long offset, int fetchMaxRowCount) throws NoSuchStatementException, MissingResultsException {
        return null;
    }

    @Override
    public boolean syncResults(StatementHandle sh, QueryState state, long offset) throws NoSuchStatementException {
        return false;
    }

    @Override
    public void commit(ConnectionHandle ch) {

    }

    @Override
    public void rollback(ConnectionHandle ch) {

    }
}
