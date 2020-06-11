package com.cisco.wap.jdbc;

import com.cisco.wap.StoreRequest;
import com.cisco.wap.StoreResponse;
import com.cisco.wap.Type;
import com.cisco.wap.VoldemortServiceGrpc;
import com.google.common.collect.Lists;
import org.apache.calcite.avatica.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.TimeZone;

public class VoldemortResultSet extends AvaticaResultSet {
    public VoldemortResultSet(AvaticaStatement statement,
                              QueryState state,
                              Meta.Signature signature,
                              ResultSetMetaData resultSetMetaData,
                              TimeZone timeZone,
                              Meta.Frame firstFrame) {
        super(statement, state, signature, resultSetMetaData, timeZone, firstFrame);
    }

    @Override
    protected AvaticaResultSet execute() throws SQLException {
        VoldemortConnection connection = (VoldemortConnection) this.statement.getConnection();
        //TODO: whether i need to create blockingStub in each call? or just here?
        VoldemortServiceGrpc.VoldemortServiceBlockingStub stub
                = VoldemortServiceGrpc.newBlockingStub(connection.getChannel());

        StoreRequest request = StoreRequest.newBuilder()
                .setType(Type.DIRECT)
                .setTable("FirstRun")
                .setKey("cat")
                .build();

        StoreResponse storeResponse = stub.get(request);

        columnMetaDataList.clear();

        List<ColumnMetaData> metaData = Lists.newArrayList();
        Class columnClass = Integer.class;
        ColumnMetaData.ScalarType type = ColumnMetaData.scalar(Types.INTEGER, columnClass.getCanonicalName(),
                ColumnMetaData.Rep.of(columnClass));
        ColumnMetaData node = new ColumnMetaData(1, false, false, false,
                false, 0, false, 10, "Node ID", "nodeId",
                "test", 0, 10, "agg", "test", type, true,
                false, false, columnClass.getCanonicalName());
        metaData.add(node);
        ColumnMetaData value = new ColumnMetaData(2, false, false, false,
                false, 0, false, 10, "Aggregated", "result",
                "test", 0, 10, "agg", "test", type, true,
                false, false, columnClass.getCanonicalName());
        metaData.add(value);

        List<Object> result = Lists.newArrayList();
        Object[] row = new Object[2];
        row[0] = storeResponse.getNodeId();
        row[1] = Integer.parseInt(storeResponse.getPayload());
        result.add(row);

        columnMetaDataList.addAll(metaData);
        cursor = MetaImpl.createCursor(signature.cursorFactory, result);
        return super.execute2(cursor, columnMetaDataList);
    }
}
