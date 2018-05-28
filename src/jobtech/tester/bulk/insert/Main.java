/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package jobtech.tester.bulk.insert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import kooki.db.MySQLConnector;
import org.apache.commons.configuration.ConfigurationException;

/**
 *
 * @author Wee Tiong ANG <weetiong@jobtech.sg>
 */
public class Main 
{
    private static final int INSERTS = 1000000;
    private static final String INSERT_SQL = "INSERT INTO jobtech_talent_management.test_insert_table (data) VALUES (?)";
    private static final String DATA = "abcdefghijklmnopqrstuvwxyz0123456789";
    private static MySQLConnector connector;
    
    public static void main(String[] args) 
    {
        try 
        {
            connector = new MySQLConnector("conf/db-config-jobtech-microc.xml");
            Main main = new Main();
            main.insertNormal();
            main.insertTransaction();
            main.insertBatch();
            main.insertTransactionBatch();
        } 
        catch (ConfigurationException | ClassNotFoundException | SQLException ex) 
        {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void insertNormal() throws SQLException
    {
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection(connector.getConnectionString()))
        {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL))
            {
                for (int i = 0; i < INSERTS; i ++)
                {
                    statement.setString(1, DATA);
                    statement.executeUpdate();
                }
            }
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        String totalTimeString = String.format
        (
            "%d min, %d sec",
            TimeUnit.MILLISECONDS.toMinutes(totalTime),
            TimeUnit.MILLISECONDS.toSeconds(totalTime) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTime))
        );
        
        System.out.println(totalTimeString);
        // 9 min, 45 sec    - 100,000
        // 46 min, 59 sec   - 200,000
        // 53 min, 32 sec   - 500,000
        // 97 min, 49 sec   - 1,000,000
    }
    
    public void insertTransaction() throws SQLException
    {
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection(connector.getConnectionString()))
        {
            connection.setAutoCommit(false);
            
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL))
            {
                for (int i = 0; i < INSERTS; i ++)
                {
                    statement.setString(1, DATA);
                    statement.executeUpdate();
                }
            }
            
            connection.commit();
            connection.setAutoCommit(true);
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        String totalTimeString = String.format
        (
            "%d min, %d sec",
            TimeUnit.MILLISECONDS.toMinutes(totalTime),
            TimeUnit.MILLISECONDS.toSeconds(totalTime) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTime))
        );
        
        System.out.println(totalTimeString);
        // 9 min, 47 sec
        // 48 min, 12 sec
        // 51 min, 56 sec
        // 97 min, 54 sec
    }
    
    public void insertBatch() throws SQLException
    {
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection(connector.getConnectionString()))
        {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL))
            {
                for (int i = 0; i < INSERTS; i ++)
                {
                    statement.setString(1, DATA);
                    statement.addBatch();
                }
                
                statement.executeBatch();
            }
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        String totalTimeString = String.format
        (
            "%d min, %d sec",
            TimeUnit.MILLISECONDS.toMinutes(totalTime),
            TimeUnit.MILLISECONDS.toSeconds(totalTime) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTime))
        );
        
        System.out.println(totalTimeString);
        // 0 min, 6 sec
        // 0 min, 12 sec
        // 0 min, 17 sec
        // 0 min, 36 sec
    }
    
    public void insertTransactionBatch() throws SQLException
    {
        long startTime = System.currentTimeMillis();
        
        try (Connection connection = DriverManager.getConnection(connector.getConnectionString()))
        {
            connection.setAutoCommit(false);
            
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL))
            {
                for (int i = 0; i < INSERTS; i ++)
                {
                    statement.setString(1, DATA);
                    statement.addBatch();
                }
                
                statement.executeBatch();
                connection.commit();
                connection.setAutoCommit(true);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        String totalTimeString = String.format
        (
            "%d min, %d sec",
            TimeUnit.MILLISECONDS.toMinutes(totalTime),
            TimeUnit.MILLISECONDS.toSeconds(totalTime) - 
            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(totalTime))
        );
        
        System.out.println(totalTimeString);
        // 0 min, 2 sec
        // 0 min, 16 sec
        // 0 min, 17 sec
        // 0 min, 37 sec
    }
}
