package com.cisco.wap;

import com.cisco.wap.jdbc.Driver;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DriverTest {
    @Test
    public void testDriver() throws SQLException {
        Driver driver = new Driver();
        Connection connect = driver.connect("jdbc:voldemort://localhost:6666", null);
        Statement statement = connect.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT 1");
        while(resultSet.next()) {
            int nodeId = resultSet.getInt(1);
            int value = resultSet.getInt(2);
            System.out.println(nodeId+"@"+value);
        }
    }
}
