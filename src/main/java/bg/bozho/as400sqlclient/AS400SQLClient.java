package bg.bozho.as400sqlclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.ibm.as400.access.AS400JDBCDriver;

import de.vandermeer.asciitable.AsciiTable;

@SpringBootApplication
public class AS400SQLClient {
    
    public static void main(String[] args) throws Exception {
        
        if (args.length != 2) {
            System.out.println("Usage: java -jar as400-sql-client.jar <connectionString> <username>");
            System.exit(0);
        }

        LineReader lineReader = LineReaderBuilder.builder().build();
        
        System.out.print("Password: ");
        String password = lineReader.readLine('0');
        
        DriverManager.registerDriver(new AS400JDBCDriver());
        
        String connectionString = args[0];
        String username = args[1];
        
        System.out.println("Trying to connect to " + connectionString + " with username: " + username);
        
        Connection connection = DriverManager.getConnection(connectionString, username, password);
        Statement statement = connection.createStatement();
        
        System.out.println("Connected to the target server. Type your queries");
        System.out.println();
        
        while (true) {
            System.out.print("> ");
            try {
                String query = lineReader.readLine();
                if (query.equals("quit")) {
                    break;
                } else if (query.toLowerCase().startsWith("select")) {
                    ResultSet rs = statement.executeQuery(query);
                    AsciiTable table = new AsciiTable();
                    List<String> columns = new ArrayList<>();
                    ResultSetMetaData meta = rs.getMetaData(); 
                    for (int i = 1; i <= meta.getColumnCount(); i ++) {
                        columns.add(meta.getColumnName(i) + " (" + meta.getColumnType(i) + ")");
                    }
                    table.addRow(columns);
                    
                    while (rs.next()) {
                        List<String> row = new ArrayList<>();
                        for (int i = 1; i <= meta.getColumnCount(); i ++) {
                            Object obj = rs.getObject(i);
                            if (obj instanceof byte[]) {
                                if (((byte[]) obj).length > 300) {
                                    row.add("LOB");
                                } else {
                                    row.add(new String((byte[]) obj));
                                }
                            } else {
                                row.add(String.valueOf(obj));
                            }
                        }
                        table.addRow(row);
                    }
                    table.getContext().setWidth(columns.size() * 15);
                    System.out.println(table.render());
                } else {
                    int affected = statement.executeUpdate(query);
                    System.out.println("Query executed, affected rows: " + affected);
                }
            } catch (SQLException ex) {
                System.out.println("Failed to execute query:" + ex.getMessage());
            }
        }
    }
}
