// Пробное подключение
package org.example;

import java.sql.*;

public class App {
    public static void main(String[] args) {
        String url = "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1";
        
        try (Connection conn = DriverManager.getConnection(url, "sa", "");
             Statement stmt = conn.createStatement()) {
            
            System.out.println("Подключение к H2 успешно!");

            stmt.execute("CREATE TABLE USERS (id INT PRIMARY KEY, name VARCHAR(50))");
            stmt.execute("INSERT INTO USERS VALUES (1, 'Кто-то Какой-то'), (2, 'Левый Правый')");

            ResultSet rs = stmt.executeQuery("SELECT * FROM USERS");
            while (rs.next()) {
                System.out.println(rs.getInt("id") + ": " + rs.getString("name"));
            }
        } catch (SQLException e) {
            System.err.println("Ошибка SQL:");
            e.printStackTrace();
        }
    }
}