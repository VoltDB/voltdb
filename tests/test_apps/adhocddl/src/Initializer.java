import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import org.voltdb.CLIConfig;


public class Initializer
{
    static class InitConfig extends CLIConfig {
        @Option(desc = "Number of tables in server")
        int numOfTables = 20;

        @Option(desc = "Table name prefix")
        String prefix = "T";
    }

    public static void main(String[] args) throws IOException
    {
        InitConfig config = new InitConfig();
        DDLGenerator DDLGen = new DDLGenerator();
        FileOutputStream fos = new FileOutputStream(new File("ddl.sql"));

        for(int i = 0; i < config.numOfTables; i++)
        {
            String ddl = DDLGen.CreateTable(i, config.prefix) + "\n";
            fos.write(ddl.getBytes());
        }
    }
}
