package com.cisco.wap.jdbc;

import com.cisco.wap.utils.Tuple2;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.apache.calcite.avatica.*;

import java.sql.SQLException;
import java.util.*;

public class VoldemortConnection extends AvaticaConnection {
    public static final int DEFAULT_PORT = 6666;
    private ManagedChannel channel;

    protected VoldemortConnection(UnregisteredDriver driver,
                                  VoldemortJdbcFactory factory,
                                  String url,
                                  Properties info) throws SQLException {
        super(driver, factory, url, info);
        Tuple2<String, Integer> addressInfo = getUrlAndPort(url);
        try {
            channel = ManagedChannelBuilder.forAddress(addressInfo._1(), addressInfo._2())
                    .usePlaintext()
                    .build();
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    public Meta.Signature mockPreparedSignature(String sql) {
        List<AvaticaParameter> params = new ArrayList<AvaticaParameter>();
        int startIndex = 0;
        while (sql.indexOf("?", startIndex) >= 0) {
            AvaticaParameter param = new AvaticaParameter(false, 0, 0, 0, null, null, null);
            params.add(param);
            startIndex = sql.indexOf("?", startIndex) + 1;
        }

        ArrayList<ColumnMetaData> columns = new ArrayList<ColumnMetaData>();
        Map<String, Object> internalParams = Collections.<String, Object> emptyMap();

        return new Meta.Signature(columns, sql, params, internalParams, Meta.CursorFactory.ARRAY, Meta.StatementType.SELECT);
    }

    private Tuple2<String, Integer> getUrlAndPort(String url) {
        String prefix = Driver.CONNECT_STRING_PREFIX + "[[A-Za-z0-9]*=[A-Za-z0-9]*;]*//";
        url = url.replaceAll(prefix, "");
        String[] address =  url.split(":");
        int port = DEFAULT_PORT;
        if(Objects.nonNull(address[1])) {
            port = Integer.parseInt(address[1]);
        }
        return new Tuple2<>(address[0], port);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return true;
    }

    @Override
    public void close() throws SQLException {
        try {
            this.channel.shutdown();
        } catch (Exception e) {
            throw new SQLException(e.getMessage(), e);
        }
    }

    public ManagedChannel getChannel() {
        return channel;
    }
}
