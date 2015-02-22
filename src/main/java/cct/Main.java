package cct;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.DatabaseConfiguration;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.*;
import java.util.Iterator;

public class Main {
    public static void main(String[] args) throws SQLException {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DriverManager.getConnection("jdbc:derby:;shutdown=true");
                } catch (SQLException e) {
                }
            }
        }));

        // create a DataSource
        String url = "jdbc:derby:memory:myDB;create=true";
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driver);
        dataSource.setUrl(url);

        // create schema and populate data into on-memory Derby
        try (Connection cn = dataSource.getConnection()) {
            try (Statement st = cn.createStatement()) {
                st.executeUpdate("create table app_config (name varchar(255) primary key, value varchar(255))");
            }
            try (PreparedStatement ps = cn.prepareStatement("insert into app_config (name, value) values (?, ?)")) {
                addBatch(ps, "anyJob.someProp1", "abc");
                addBatch(ps, "anyJob.someProp2", "gef");
                addBatch(ps, "someJob.list", "1,2,3,4,5");
                addBatch(ps, "someJob.someProp", "${anyJob.someProp1}");
                ps.executeBatch();
            }
        }

        Configuration config = new DatabaseConfiguration(dataSource, "app_config", "name", "value");

        // Obtain all of keys
        for (Iterator<String> keys = config.getKeys(); keys.hasNext(); ) {
            String key = keys.next();
            Object value = config.getProperty(key);
            System.out.println(key + "=" + value);
        }

        // Obtain keys with particular prefix
        System.out.println();
        for (Iterator<String> keys = config.getKeys("someJob"); keys.hasNext(); ) {
            String key = keys.next();
            Object value = config.getProperty(key);
            System.out.println(key + "=" + value);
        }

        // Obtain list
        System.out.println();
        System.out.println(config.getList("someJob.list")); // [1, 2, 3, 4, 5]
        System.out.println(config.getString("someJob.list")); // 1

        // Referencing a value of other key
        System.out.println();
        System.out.println(config.getString("someJob.someProp")); // abc

        // update value of a row
        try (Connection cn = dataSource.getConnection()) {
            try (Statement st = cn.createStatement()) {
                st.executeUpdate("update app_config set value='hij' where name='anyJob.someProp1'");
            }
        }

        // check updated value
        System.out.println();
        System.out.println(config.getString("anyJob.someProp1")); // hij
        System.out.println(config.getString("someJob.someProp")); // hij
    }

    static void addBatch(PreparedStatement ps, String name, String value) throws SQLException {
        ps.setString(1, name);
        ps.setString(2, value);
        ps.addBatch();
    }
}
