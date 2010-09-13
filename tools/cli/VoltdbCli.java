/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * The point of this program is to create a "real" interface to VoltDB.
 * The supported commands are:
 *    COMPILE
 *    LOAD DATABASE
 *    CONNECT
 *    SHUTDOWN
 *    SAVE
 *    RESTORE
 *    SELECT ... (SQL query)
 *    INSERT ... (SQL query)
 *    DELETE ... (SQL query)
 *    UPDATE ... (SQL query)
 */

package cli;

import java.io.*;
import java.math.BigDecimal;
import java.util.Scanner;
import org.voltdb.*;
import org.voltdb.client.ClientFactory;

public class VoltdbCli {
    static String databasename = null;
    static String username = null;
    static String password = null;
    static String voltdbroot = null;
    static org.voltdb.client.Client database;
    static String classpathroot = null;
    static String voltdbjarpath =null;
    static String voltdbsopath = null;


    static Scanner scanner = new Scanner(System.in);

    public static void main (String[] args) {
            // First, make sure things are defined enough to be able
            // to execute the commands.
        initcli();
        if (args.length > 0) {
            // we have a command string. Rebuild the original string and
            // pass it to our interpreter. (This is wasted processing,
            // but saves writing special conditions into the interpreter.)
            String fullstring = "";
            for (String s: args) {
                if (s.indexOf(' ')>-1) {
                    fullstring += " \"" + s + "\"";
                } else {
                    fullstring += " " + s;
                }
            }
            fullstring = fullstring.trim();
            //System.out.println("Processing Fullstring: " + fullstring);
            interpreter(fullstring);
        } else {
               while (interpreter(getcli())) {
                   //Do nothing Just keep prompting for input.
               }
               System.out.println("Goodbye!");

        }
    }
    static void initcli() {
        voltdbroot = System.getenv("VOLTDBROOT");
        String filename;
        if ((voltdbroot == null) || (voltdbroot.compareTo("") == 0) ) {
            warningmessage("The system environment variable VOLTDBROOT is not defined.\n"+
                    "Not all commands will be available to you. See HELP for more information.");
            return;
        }
        if (voltdbroot.substring(voltdbroot.length()-1).compareTo("/") != 0) {
            voltdbroot = voltdbroot + "/";
        }
                // Now determine the location of the classpath and the .jar and .so files,
        classpathroot = System.getenv("CLASSPATH");
        File dir = new File(voltdbroot+"voltdb/");
        String[] children = dir.list();
        if (children == null) {
                warningmessage("VOLTDBROOT is not a valid VoltDB installation directory.\n" +
                        "Not all commands will be available to you. See HELP for more information.");
                return;
        } else {
            for (int i=0; i<children.length; i++) {
                    // Get filename of file or directory
                filename = children[i];
                //System.out.println("Checking file " + filename);
                if (filename.substring(filename.length()-4).compareToIgnoreCase(".jar")==0) {
                    voltdbjarpath = voltdbroot+"voltdb/"+filename;
                    //System.out.println("Found JAR file " + voltdbjarpath);
                }
                if (filename.substring(filename.length()-3).compareToIgnoreCase(".so")==0) {
                    voltdbsopath = voltdbroot+"voltdb/";
                    //System.out.println("Found library file " + voltdbsopath);
                }
            }
         }

        if (voltdbjarpath == null) warningmessage("Cannot find VoltDB software .jar file.\n" +
        "COMPILE and LOAD will not be available. See HELP for more information.");
        if (voltdbsopath == null) warningmessage("Cannot find VoltDB software .so library file.\n" +
        "LOAD will not be available. See HELP for more information.");


        //debugmsg("Init - voltdbroot=[" + voltdbroot + "]");
    }
   static String getcli() {
       String cli = null;
       String token = null;
       int i;
       while (cli==null || (cli.compareTo("") ==0)) {
           System.out.print("VoltDB> ");
           cli = scanner.nextLine().trim();
       }
               // Need to wait for semicolon on SQL commands
       i = cli.indexOf(" ");
       if (i<0) {
           token = cli;
       } else {
           token = cli.substring(0,i);
       }
       if (    (token.compareToIgnoreCase("select")==0) ||
               (token.compareToIgnoreCase("insert")==0) ||
               (token.compareToIgnoreCase("update")==0) ||
               (token.compareToIgnoreCase("delete")==0)
                       ) {
           // iterate until last character is ";"
           while(cli.substring(cli.length()-1).compareTo(";") != 0) {
               System.out.print("_ ");
               token = scanner.nextLine().trim();
               if (token.length() >0) { cli = cli + " " + token; }
           }
       }

       return cli;


   }
   static boolean interpreter(String s) {
       String token;
       String[] tokens;

       int i;
       i = s.indexOf(" ");
       if (i<0) {
           token = s;
           s = "";
       } else {
           token = s.substring(0,i);
           s = s.substring(i).trim();
       }
       tokens = tokenize(s);


               // Interpret the first token
       if (token.compareToIgnoreCase("compile")==0) {
           int nodes;
           int partitions;
           String host;
           if (tokens[0]==null) {
               errormessage("You must specify an input file name.");
               return(true);
           }
           if (tokens[1]==null) {
               errormessage("You must specify an output file name.");
               return(true);
           }
           if (tokens[2]==null) {
               host = "localhost";
           } else {
               host = tokens[2];
           }
           if (tokens[3]==null) {
               nodes = 1;
           } else {
               try { nodes = Integer.parseInt(tokens[4]);
               }
               catch (Exception e) {
                   errormessage("Number of nodes is not a valid number.");
                   return(true);
               }
           }
           if (tokens[4]==null) {
               partitions = 1;
           } else {
               try { partitions = Integer.parseInt(tokens[4]);
               }
               catch (Exception e) {
                   errormessage("Number of partitions is not a valid number.");
                   return(true);
               }
           }
           voltCompile(tokens[0],tokens[1],host,nodes,partitions);
           return(true);
       }
       if ( (token.compareToIgnoreCase("start")==0) ||
               (token.compareToIgnoreCase("startup")==0) ) {
           voltLoad(tokens[0]);
           return(true);

       }
       if (token.compareToIgnoreCase("connect")==0) {
           String host = tokens[0];
           String username = tokens[1];
           String password = tokens[2];

           if (host==null) { host = "localhost"; }
           if (username==null) { username=""; }
           if (password==null) { password=""; }

           voltConnect(host,username,password);
           return(true);

       }
       if (token.compareToIgnoreCase("shutdown")==0) {
           voltShutdown();
           return(true);

       }
       if (    (token.compareToIgnoreCase("select")==0) ||
               (token.compareToIgnoreCase("insert")==0) ||
               (token.compareToIgnoreCase("update")==0) ||
               (token.compareToIgnoreCase("delete")==0)
                       ) {
           voltAdHoc(token + " " + s);
           return(true);

       }

       if (token.compareToIgnoreCase("save")==0) {
           debugmsg(token.toUpperCase());
           return(true);

       }
       if (token.compareToIgnoreCase("restore")==0) {
           debugmsg(token.toUpperCase());
           return(true);

       }
       if (token.compareToIgnoreCase("help")==0) {
           helpsystem(tokens[0]);
           return(true);

       }
       if (token.compareToIgnoreCase("exit")==0) {
              return false;
       }

       errormessage("%ERROR - unrecognized command: " + token.toUpperCase());
       return true;

   }

   //****************************** Commands *******************************
   static void voltCompile(String projectdef, String catalog, String host,
           Integer numofnodes, Integer numofpartitions) {
       String cmd;
       String fullclasspath;

       if (!checkpaths()) return;

       if ( (classpathroot != null) && (classpathroot != "")) {
           fullclasspath = classpathroot + ":";
       } else {
           fullclasspath = "";
       }
       fullclasspath = fullclasspath + ".:" + voltdbjarpath;

       cmd = "java"
                   + " -classpath " + fullclasspath
                   + " org.voltdb.compiler.VoltCompiler ";
       cmd = cmd + projectdef + " " + numofnodes.toString()
                              + " " + numofpartitions.toString()
                              + " " + host
                              + " " + catalog;
       voltShell(cmd);
  }
    static void voltLoad(String catalog) {
        String cmd;
        String fullclasspath;

       if (!checkpaths()) return;

       if ( (classpathroot != null) && (classpathroot != "")) {
           fullclasspath = classpathroot + ":";
       } else {
           fullclasspath = "";
       }
       fullclasspath = fullclasspath + ".:" + voltdbjarpath + ":" + catalog;
       cmd = "java -Djava.library.path=" + voltdbsopath
            + " -classpath " + fullclasspath
            + " org.voltdb.VoltDB catalog ";
       cmd = cmd + catalog;
       voltShell(cmd);
   }
    static void voltConnect(String dbnode, String username, String password) {
        String errmsg = null;
        boolean started = false;
        if (username == null) { username = ""; }
        if (password == null) { password = ""; }
          database = ClientFactory.createClient();
          //System.out.printf("About to connect to [%s] [%s] [%s] \n",dbnode,username,password);
          try {
              database.createConnection(dbnode, username, password);
              started = true;
          }
          catch (java.net.ConnectException e) {
              errmsg = "Database server not running.";
          }
          catch (IOException e) {
              errmsg = "Failed to connect. I/O error: " + e.getMessage();
          }
          catch (Exception e) {
              errmsg = "Failed to connect. Generic error: " + e.getMessage();
              e.printStackTrace();
          }
          if (!started) {
              database = null;
              databasename = null;
              errormessage(errmsg);
          } else {
              infomessage("Connection established.");
          }


    }
    static void voltShutdown() {
       if (!checkConnection()) return;
       try {
           database.callProcedure("@Shutdown");
       }
       catch (Exception e)
       {
           // we expect an error here. just ignore it.
       }
       infomessage("Shutdown initiated. Connection closed.");
       databasename = null;
       database = null;


   }
   static void voltSave(String loc, String guid) {
       if (!checkConnection()) return;
   }
   static void voltRestore(String loc, String guid) {
       if (!checkConnection()) return;
   }
   static void voltAdHoc(String sql) {
       VoltTable[] results;
       if (!checkConnection()) return;
       try {
           results = database.callProcedure("@adhoc", sql);
           if (results.length==1) { parseresults(results[0]); };
       }
       catch (Exception e)
       {
           errormessage("SQL statement failed for the following reason:\n   " +
                   e.getMessage());
           return;
       }
           // need to interpret and print the results.
   }
   static void voltShell(String cmd) {
       Process proc;
       String stats;
       String tmpstr;
       System.out.println("Executing shell command: " + cmd);
       try {
           proc = Runtime.getRuntime().exec(cmd);
           BufferedReader stdin = new BufferedReader(new InputStreamReader(proc.getInputStream()));
           BufferedReader stderr = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
               // Display the output.
           while ( (stats = stdin.readLine()) != null ) {
               System.out.println(stats);
           }
                   // Display any error messages.
           tmpstr = "";
           while ( (stats = stderr.readLine()) != null ) {
              tmpstr = tmpstr + stats + "\n";
           }
           if (tmpstr.length() > 0) {
               System.out.print(tmpstr);
           }
       }
       catch (Exception e) {
           errormessage("Cannot execute command. Reason: " + e.getMessage());
       }
  }
   static void parseresults(VoltTable results) {
           // Determine what the column names are.
       int cols = results.getColumnCount();
       int rows = results.getRowCount();
       Integer i;
       Integer j;
       Integer maxrownamesize = 0;
       VoltTableRow row;
       String[] colnames = new String[cols];
       for (i=0;i<cols;i++) {
           colnames[i]=results.getColumnName(i);
           if (colnames[i].length() > maxrownamesize) maxrownamesize = colnames[i].length();
       }
       for (i=0;i<rows;i++) {
           out("****** Row " + i.toString() + " *****");
           row = results.fetchRow(i);
           String val;
           for (j=0;j<cols;j++) {
               switch(results.getColumnType(j)) {
               case TINYINT:
               case SMALLINT:
               case BIGINT:
               case INTEGER:
                   Long di = row.getLong(j);
                   val = di.toString();
                   break;
               case STRING:
                   val = row.getString(j);
                   break;
               case DECIMAL:
                   BigDecimal dbd = row.getDecimalAsBigDecimal(j);
                   val = dbd.toString();
                   break;
               case FLOAT:
                   Double ddbl = row.getDouble(j);
                   val = ddbl.toString();
                   break;
               default:
                  val = "(Unresolved datatype " + results.getColumnType(j).toString() + ")";
                  break;

               }
               out("   " + flushleft(colnames[j],maxrownamesize) + ": " + val);
           }
       }
   }
   static String flushleft(String s, int l) {
       String tmp;
       String padding = "                                                               ";
       tmp = s+padding;
       if (tmp.length()>l) { tmp = tmp.substring(0,l); }
       if (tmp.length()<l) { tmp = tmp+padding.substring(tmp.length()); }
       return tmp;
    }
   //********************************* Help System ***************************
   static void noop() {
       out("%Sorry. That hasn't been implemented yet.");
   }
   static void out (String s) {
       System.out.println(s);
   }
   static void helpsystem(String cmd) {
       String tmpstr;
       if ((cmd == null) || (cmd == "")) {
               // Display list of commands.
           out("The VoltDB command line helps you perform common tasks for managing VoltDB databases. Usage:");
           out("\nVoltDB command [arguments]\n");
           out("You can issue a single command using 'VoltDB' followed by a command from the shell prompt " +
                   "or use 'VoltDB' by itself t enter prompt mode to execute multiple commands. " +
                   "Use EXIT to return to the shell.");
           out("\nSupported commands:\n");
           out("    COMPILE");
           out("    CONNECT");
           out("    RESTORE");
           out("    SAVE");
           out("    SHUTDOWN");
           out("    STARTUP");
           out("\nSQL commands:\n");
           out("    DELETE");
           out("    INSERT");
           out("    SELECT");
           out("    UPDATE");
           out("\nUse HELP <command>. to get more information about a specific command.");
           out("\nNote: you  must define the system environment variable VOLTDBROOT " +
                   "pointing to the directory where you installed VoltDB before using " +
                   "many of the preceding commands. Use HELP VOLTDB for more information " +
                   "about configuring the command line utility.");
       } else {
           tmpstr = cmd.toUpperCase();
           if (tmpstr.compareTo("COMPILE")==0)  {
               out ("Compiles the project definition file into a runtime catalog.");
               out ("\nUsage: COMPILE input-file output-file [node [num-of-nodes [num-of-partitions]]]");
           }
           if (tmpstr.compareTo("CONNECT")==0)  {
               out ("creates a connection to a database cluster.");
               out ("\nUsage: CONNECT node [username password]");
           }
           if ( (tmpstr.compareTo( "START") == 0) || (tmpstr.compareTo("STARTUP") == 0 ) )  {
               out ("Starts the database.");
               out ("\nUsage: STARTUP catalog-file");
           }
           if (tmpstr.compareTo("RESTORE")==0)  {
               out ("Restores the database from a previously saved backup.");
               out ("\nUsage: RESTORE directory-path unique-id");
           }
           if (tmpstr.compareTo("SAVE") == 0 )  {
               out ("Saves the database to disk.");
               out ("\nUsage: SAVE directory-path unique-id");
           }
           if (tmpstr.compareTo("SHUTDOWN") == 0 )  {
               out ("Shuts down the database.");
               out ("\nUsage: SHUTDOWN");
           }
           if (tmpstr.compareTo("DELETE") == 0)  {
               out ("Executes an SQL DELETE statement on the currently open database.");
               out ("\nUsage: DELETE FROM table-name [WHERE expression...];");
           }
           if (tmpstr.compareTo("INSERT")==0)  {
               out ("Executes an SQL INSERT statement on the currently open database.");
               out ("\nUsage: INSERT INTO table-name [(column-name[,...])] VALUES (value[,...]);");
           }
           if (tmpstr.compareTo("SELECT")==0)  {
               out ("Executes an SQL SELECT query on the currently open database.");
               out ("\nUsage: SELECT expression [,...] FROM table-name [AS alias] [,...] [constraint [...] ];");
           }
           if (tmpstr.compareTo("UPDATE")==0)  {
               out ("Executes an SQL UPDATE statement on the currently open database.");
               out ("\nUsage: UPDATE table-name SET column=value[,..] [WHERE expression...]");
           }
       }

   }
   //********************************* Utility routines **********************
   static String[] tokenize(String s) {
       String[] tokens = new String[10];
       int i;
       int currenttoken = 0;

       for (i=0;i<10;i++) {
          tokens[i]=null;
       }
       while( (s.length() > 0) && (currenttoken < 10) ) {
           i = s.indexOf(" ");
           if (i>0) {
               tokens[currenttoken] = s.substring(0,i);
               currenttoken++;
               s = s.substring(i+1).trim();
           } else {
               tokens[currenttoken] = s;
               s = "";
           }
       }
       return tokens;

   }
   static boolean checkConnection() {
       if (database != null) { return true; }
       else {
           errormessage ("No database connection exists. Cannot perform that function.\n" +
                   "Use the CONNECT command to create a database connection. Use HELP for more information.");
           return false;
       }
   }
   static boolean checkpaths() {
       if ( (voltdbroot != null) &&
            (voltdbsopath != null) &&
            (voltdbjarpath != null) ) { return true; }
       else {
           errormessage ("Cannot establish classpath for the VoltDB jar and library files.\n" +
                   "You must define VOLTDBROOT to use this command. Use HELP for more information.");
           return false;
       }
   }
   static void debugmsg ( String s) {
       System.out.println("WARNING: Command " + s + " not implemented yet.");
   }
   static void warningmessage ( String s) {
       System.out.println("WARNING: " + s);
   }
   static void errormessage ( String s) {
       System.out.println("ERROR: " + s);
   }
   static void infomessage ( String s) {
       System.out.println("%" + s);
   }
   static void fatalerror ( String s) {
       System.out.println("FATAL: " + s + "\nExiting...");
       System.exit(-1);
   }

}

