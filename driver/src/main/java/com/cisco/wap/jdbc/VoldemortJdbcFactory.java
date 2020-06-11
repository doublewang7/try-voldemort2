package com.cisco.wap.jdbc;

import org.apache.calcite.avatica.*;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

public class VoldemortJdbcFactory implements AvaticaFactory {
    public static class Version40 extends VoldemortJdbcFactory {
        public Version40() {
            super(4, 0);
        }
    }

    public static class Version41 extends VoldemortJdbcFactory {
        public Version41() {
            super(4, 1);
        }
    }

    final int major;
    final int minor;

    public VoldemortJdbcFactory(int major, int minor) {
        this.major = major;
        this.minor = minor;
    }

    @Override
    public int getJdbcMajorVersion() {
        return this.major;
    }

    @Override
    public int getJdbcMinorVersion() {
        return this.minor;
    }

    @Override
    public AvaticaConnection newConnection(UnregisteredDriver unregisteredDriver,
                                           AvaticaFactory avaticaFactory,
                                           String url,
                                           Properties properties) throws SQLException {
        return new VoldemortConnection(unregisteredDriver,
                (VoldemortJdbcFactory) avaticaFactory,
                url,
                properties);
    }

    @Override
    public AvaticaStatement newStatement(AvaticaConnection avaticaConnection,
                                         Meta.StatementHandle statementHandle,
                                         int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        return new VoldemortStatement((VoldemortConnection) avaticaConnection,
                statementHandle,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public AvaticaPreparedStatement newPreparedStatement(AvaticaConnection avaticaConnection,
                                                         Meta.StatementHandle statementHandle,
                                                         Meta.Signature signature,
                                                         int resultSetType,
                                                         int resultSetConcurrency,
                                                         int resultSetHoldability) throws SQLException {
        return new VoldemortPreparedStatement(avaticaConnection,
                statementHandle,
                signature,
                resultSetType,
                resultSetConcurrency,
                resultSetHoldability);
    }

    @Override
    public AvaticaResultSet newResultSet(AvaticaStatement avaticaStatement,
                                         QueryState queryState,
                                         Meta.Signature signature,
                                         TimeZone timeZone,
                                         Meta.Frame frame) throws SQLException {
        AvaticaResultSetMetaData resultSetMetaData = new AvaticaResultSetMetaData(avaticaStatement,
                null,
                signature);
        return new VoldemortResultSet(avaticaStatement,
                queryState,
                signature,
                resultSetMetaData,
                timeZone,
                frame);
    }

    @Override
    public AvaticaDatabaseMetaData newDatabaseMetaData(AvaticaConnection avaticaConnection) {
        return new VoldemortDatabaseMetaData(avaticaConnection);
    }

    @Override
    public ResultSetMetaData newResultSetMetaData(AvaticaStatement avaticaStatement,
                                                  Meta.Signature signature) throws SQLException {
        return new AvaticaResultSetMetaData(avaticaStatement,
                null,
                signature);
    }
}
