/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.processtools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;

import org.voltcore.logging.VoltLogger;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SSHTools {
    protected static final VoltLogger cmdLog = new VoltLogger("REMOTECMD");
    private final String m_username;
    private final String m_keyFile;

    public SSHTools(String username, String key) {
        // If a username is not specified, default to the user who started the process.
        if (username == null || username.isEmpty()) {
            m_username = System.getProperty("user.name");
        } else {
            m_username = username;
        }
        // If a private key is not specified, default to the rsa key in the default location.
        if (key == null || key.isEmpty()) {
            m_keyFile = System.getProperty("user.home") + "/.ssh/id_rsa";
        } else {
            m_keyFile = key;
        }
    }

    // Execute a remote SSH command on the specified host.
    public String cmd(String hostname, String command) {
        return cmdSSH(m_username, m_keyFile, hostname, command);
    }

    // Execute a remote SSH command on the specified host.
    public String cmd(String hostname, String[] command) {
        return cmdSSH(m_username, m_keyFile, hostname, command);
    }

    public String cmdSSH(String user, String key, String host, String command) {
        return cmdSSH(user, null, key, host, command);
    }

    public String cmdSSH(String user, String key, String host, String[] command) {
        return cmdSSH(user, key, host, stringify(command));
    }

    public SFTPSession getSftpSession(String hostname, VoltLogger logger) {
        return new SFTPSession(m_username, m_keyFile, hostname, logger);
    }

    public SFTPSession getSftpSession(String user, String password, String key, String hostname, VoltLogger logger) {
        return new SFTPSession(user, password, key, hostname, logger);
    }

    public SFTPSession getSftpSession(String user, String password, String key, String hostname, int port, VoltLogger logger) {
        return new SFTPSession(user, password, key, hostname, port, logger);
    }

    /*
     * The code from here to the end of the file is code that integrates with an external
     * SSH library (JSCH, http://www.jcraft.com/jsch/).  If you wish to replaces this
     * library, these are the methods that need to be re-worked.
     */
    public String cmdSSH(String user, String password, String key, String host, String command) {
        StringBuilder result = new StringBuilder(2048);
        try{
            JSch jsch=new JSch();

            // Set the private key
            if (null != key)
                jsch.addIdentity(key);
            Session session=jsch.getSession(user, host, 22);
            session.setTimeout(5000);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            if (password != null && !password.trim().isEmpty()) {
                session.setPassword(password);
            }

            session.connect();

            Channel channel = session.openChannel("exec");
            ((ChannelExec) channel).setCommand(command);

            // Direct stderr output of command
            InputStream err = ((ChannelExec) channel).getErrStream();
            InputStreamReader errStrRdr = new InputStreamReader(err, "UTF-8");
            Reader errStrBufRdr = new BufferedReader(errStrRdr);

            // Direct stdout output of command
            InputStream out = channel.getInputStream();
            InputStreamReader outStrRdr = new InputStreamReader(out, "UTF-8");
            Reader outStrBufRdr = new BufferedReader(outStrRdr);

            StringBuffer stdout = new StringBuffer();
            StringBuffer stderr = new StringBuffer();

            channel.connect(5000);  // timeout after 5 seconds
            while (true) {
                if (channel.isClosed()) {
                    break;
                }

                // Read from both streams here so that they are not blocked,
                // if they are blocked because the buffer is full, channel.isClosed() will never
                // be true.
                int ch;
                while (outStrBufRdr.ready() && (ch = outStrBufRdr.read()) > -1) {
                    stdout.append((char) ch);
                }
                while (errStrBufRdr.ready() && (ch = errStrBufRdr.read()) > -1) {
                    stderr.append((char) ch);
                }

                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                }
            }

            // In case there's still some more stuff in the buffers, read them
            int ch;
            while ((ch = outStrBufRdr.read()) > -1) {
                stdout.append((char) ch);
            }
            while ((ch = errStrBufRdr.read()) > -1) {
                stderr.append((char) ch);
            }

            // After the command is executed, gather the results (both stdin and stderr).
            result.append(stdout.toString());
            result.append(stderr.toString());

            // Shutdown the connection
            channel.disconnect();
            session.disconnect();
        }
        catch(Throwable e){
            e.printStackTrace();
            // Return empty string if we can't connect.
        }

        return result.toString();
    }

    public ProcessData long_running_command(String hostname, String command[], String processName, OutputHandler handler) {
        try{
            JSch jsch=new JSch();
            // Set the private key
            if (null != m_keyFile)
                jsch.addIdentity(m_keyFile);
            Session session=jsch.getSession(m_username, hostname, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect(5000); // timeout after 5 seconds.

            return new ProcessData(processName, handler, session, stringify(command));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public boolean copyFromLocal(String src, String hostNameTo, String pathTo) {
        return ScpTo(src, m_username, m_keyFile, hostNameTo, pathTo);
    }

    public boolean copyFromRemote(String dst, String hostNameFrom, String pathFrom) {
        return ScpFrom(m_username, m_keyFile, hostNameFrom, pathFrom, dst);
    }

    // The Jsch method for SCP to.
    // This code is direcly copied from the Jsch SCP sample program.
    public boolean ScpTo(String local_file, String user, String key, String host, String remote_file){

        FileInputStream fis=null;
        try{
            boolean ptimestamp = true;
            String command="scp " + (ptimestamp ? "-p" :"") +" -t "+remote_file;
            cmdLog.debug("CMD: '" + command + "'");

            JSch jsch=new JSch();
            // Set the private key
            if (null != key)
                jsch.addIdentity(key);
            Session session=jsch.getSession(user, host, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect(5000); // timeout after 5 seconds

            // exec 'scp -t rfile' remotely
            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);

            // get I/O streams for remote scp
            OutputStream out=channel.getOutputStream();
            InputStream in=channel.getInputStream();

            channel.connect();

            if(checkAck(in)!=0){
                return false;
            }

            File _lfile = new File(local_file);

            if(ptimestamp){
                command="T "+(_lfile.lastModified()/1000)+" 0";
                // The access time should be sent here,
                // but it is not accessible with JavaAPI ;-<
                command+=(" "+(_lfile.lastModified()/1000)+" 0\n");
                out.write(command.getBytes()); out.flush();
                if(checkAck(in)!=0){
                    return false;
                }
            }

            // send "C0644 filesize filename", where filename should not include '/'
            long filesize=_lfile.length();
            command="C0644 "+filesize+" ";
            if(local_file.lastIndexOf('/')>0){
                command+=local_file.substring(local_file.lastIndexOf('/')+1);
            }
            else{
                command+=local_file;
            }
            command+="\n";
            out.write(command.getBytes()); out.flush();
            if(checkAck(in)!=0){
                return false;
            }

            // send a content of lfile
            fis=new FileInputStream(local_file);
            byte[] buf=new byte[1024];
            while(true){
                int len=fis.read(buf, 0, buf.length);
                if(len<=0) break;
                out.write(buf, 0, len); //out.flush();
            }
            fis.close();
            fis=null;
            // send '\0'
            buf[0]=0; out.write(buf, 0, 1); out.flush();
            if(checkAck(in)!=0){
                return false;
            }
            out.close();

            channel.disconnect();
            session.disconnect();
        }
        catch(Exception e){
            System.out.println(e);
            try{if(fis!=null)fis.close();}catch(Exception ee){}
            return false;
        }

        return true;
    }

    // The Jsch method for SCP from.
    // This code is directly copied from the Jsch SCP sample program.  Error handling has been modified by VoltDB.
    public boolean ScpFrom(String user, String key, String host, String remote_file, String local_file){

        FileOutputStream fos=null;
        try{
            String prefix=null;
            if(new File(local_file).isDirectory()){
                prefix=local_file+File.separator;
            }

            String command="scp -f "+remote_file;
            cmdLog.debug("CMD: '" + command + "'");

            JSch jsch=new JSch();
            // Set the private key
            if (null != key)
                jsch.addIdentity(key);
            Session session=jsch.getSession(user, host, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            session.connect();

            // exec 'scp -f rfile' remotely
            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);

            // get I/O streams for remote scp
            OutputStream out=channel.getOutputStream();
            InputStream in=channel.getInputStream();

            channel.connect();
            byte[] buf=new byte[1024];

            // send '\0'
            buf[0]=0; out.write(buf, 0, 1); out.flush();

            while(true){
                int c=checkAck(in);
                if(c!='C'){
                    break;
                }

                // read '0644 '
                in.read(buf, 0, 5);

                long filesize=0L;
                while(true){
                    if(in.read(buf, 0, 1)<0){
                        // error
                        break;
                    }
                    if(buf[0]==' ')break;
                    filesize=filesize*10L+(buf[0]-'0');
                }

                String file=null;
                for(int i=0;;i++){
                    in.read(buf, i, 1);
                    if(buf[i]==(byte)0x0a){
                        file=new String(buf, 0, i);
                        break;
                    }
                }

                String destination_file = prefix==null ? local_file : prefix+file;
                cmdLog.debug("CMD: scp to local file '" + destination_file + "'");

                // send '\0'
                buf[0]=0; out.write(buf, 0, 1); out.flush();

                // read a content of lfile
                fos=new FileOutputStream(destination_file);
                int foo;
                while(true){
                    if(buf.length<filesize) foo=buf.length;
                    else foo=(int)filesize;
                    foo=in.read(buf, 0, foo);
                    if(foo<0){
                        // error
                        break;
                    }
                    fos.write(buf, 0, foo);
                    filesize-=foo;
                    if(filesize==0L) break;
                }
                fos.close();
                fos=null;

                if(checkAck(in)!=0){
                    cmdLog.debug("CMD: scp checkAck failed");
                    System.out.println("checkAck did not equal zero.");
                    return false;
                }

                // send '\0'
                buf[0]=0; out.write(buf, 0, 1); out.flush();
            }

            session.disconnect();
        }
        catch(Exception e){
            System.out.println(e);
            cmdLog.debug("CMD: scp failed with exception: " + e.toString());
            return false;
        }
        finally
        {
            try{if(fos!=null)fos.close();}catch(Exception ee){}
        }

        return true;
    }

    static int checkAck(InputStream in) throws IOException{
        int b=in.read();
        // b may be 0 for success,
        //          1 for error,
        //          2 for fatal error,
        //          -1
        if(b==0) return b;
        if(b==-1) return b;

        if(b==1 || b==2){
            StringBuffer sb=new StringBuffer();
            int c;
            do {
                c=in.read();
                sb.append((char)c);
            }
            while(c!='\n');
            if(b==1){ // error
                System.out.print(sb.toString());
            }
            if(b==2){ // fatal error
                System.out.print(sb.toString());
            }
        }
        return b;
    }

    public static String stringify(String[] str_array) {
        StringBuffer result = new StringBuffer(2048);
        if (str_array != null) {
            for (int i=0; i<str_array.length; i++) {
                result.append(" ").append(str_array[i]);
            }
        }
        return result.toString();
    }

    public static void main(String args[]) throws Exception {
        SSHTools tools = new SSHTools(args[0], args[1]);
        System.out.println(tools.cmd( args[2], args[3]));
    }
}
