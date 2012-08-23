/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.processtools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SSHTools {
    private final String m_username;
    private final String m_password;

    public SSHTools(String username, String password) {
/*
        if (username != null && username.isEmpty()) {
            m_username = null;
        } else {
            m_username = username;
        }
        m_password = password;
*/
        m_username ="jpiekos";
        m_password = "katama";
 //       m_host="Johns-MacBook-Pro.local";
   }

    // Temporary - delete when finished implementation
    private static void printCommandString(String command[])
    {
        StringBuilder sb = new StringBuilder();
        if (command != null) {
            for (String c : command) {
                sb.append(c).append(" ");
            }
        }
        else sb.append("null");
        System.out.println("JWP **********************");
        System.out.println("JWP Starting PROCESS with command " + sb.toString() );
        System.out.println("JWP **********************");
    }

    public ProcessData command(String hostname, String command[], String processName, OutputHandler handler) {
        printCommandString(command);
        try{
            JSch jsch=new JSch();
            Session session=jsch.getSession(m_username, hostname, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            // If two machines have SSH passwordless logins setup, the following line is not needed:
            session.setPassword(m_password);
            session.connect();

            return new ProcessData(processName, handler, session, stringify(command));
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return null;
        }
    }

    public boolean copyFromLocal(String src, String hostNameTo, String pathTo) {
        return ScpTo(src, m_username, m_password, hostNameTo, pathTo);
    }

    public boolean copyFromRemote(String dst, String hostNameFrom, String pathFrom) {
        return ScpFrom(m_username, m_password, hostNameFrom, pathFrom, dst);
    }

    // The Jsch method for SCP to.
    public boolean ScpTo(String local_file, String user, String password, String host, String remote_file){

        FileInputStream fis=null;
        try{
            host="Johns-MacBook-Pro.local";
            boolean ptimestamp = true;
            String command="scp " + (ptimestamp ? "-p" :"") +" -t "+remote_file;

            System.out.println("JWP SCP To: " + command);

            JSch jsch=new JSch();
            Session session=jsch.getSession(user, host, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            if (null != password)
                session.setPassword(password);
            session.connect();

            // exec 'scp -t rfile' remotely
            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);

            // get I/O streams for remote scp
            OutputStream out=channel.getOutputStream();
            InputStream in=channel.getInputStream();

            channel.connect();

            if(checkAck(in)!=0){
                System.out.println("checkAck did not equal zero.");
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
                    System.out.println("checkAck did not equal zero.");
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
                System.out.println("checkAck did not equal zero.");
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
                System.out.println("checkAck did not equal zero.");
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
    public boolean ScpFrom(String user, String password, String host, String remote_file, String local_file){

        FileOutputStream fos=null;
        try{
            String prefix=null;
            if(new File(local_file).isDirectory()){
                prefix=local_file+File.separator;
            }

            host="Johns-MacBook-Pro.local";
            String command="scp -f "+remote_file;

            System.out.println("JWP SCP From: " + command);

            JSch jsch=new JSch();
            Session session=jsch.getSession(user, host, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            if (null != password)
                session.setPassword(password);
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

                //System.out.println("filesize="+filesize+", file="+file);

                // send '\0'
                buf[0]=0; out.write(buf, 0, 1); out.flush();

                // read a content of lfile
                fos=new FileOutputStream(prefix==null ? local_file : prefix+file);
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
            try{if(fos!=null)fos.close();}catch(Exception ee){}
            return false;
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
        String result = "";
        if (str_array != null) {
            for (int i=0; i<str_array.length; i++) {
                result += " " + str_array[i];
            }
        }
        return result;
    }

    public String cmdSSH(String user, String password, String host, String[] command) {
        return cmdSSH(user, password, host, stringify(command));
    }

    public String cmdSSH(String user, String password, String host, String command) {
        String result = "";
        try{
            System.out.println("JWP EXECUTING SSH: " + command);
            host="Johns-MacBook-Pro.local";

            JSch jsch=new JSch();
            Session session=jsch.getSession(user, host, 22);

            // To avoid the UnknownHostKey issue
            java.util.Properties config = new java.util.Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);

            // If two machines have SSH passwordless logins setup, the following line is not needed:
            session.setPassword(password);
            session.connect();

            Channel channel=session.openChannel("exec");
            ((ChannelExec)channel).setCommand(command);

            // Set up the i/o streams, in, out, err
            //channel.setInputStream(System.in);
            channel.setInputStream(null);
            ((ChannelExec)channel).setErrStream(System.err);
            InputStream in=channel.getInputStream();

            channel.connect();
            byte[] tmp=new byte[1024];
            while(true){
                while(in.available()>0){
                    int i=in.read(tmp, 0, 1024);
                    if(i<0)break;
                    String string_fragment = new String(tmp, 0, i);
                    result += string_fragment;
                }
                if(channel.isClosed()){
                    // System.out.println("exit-status: "+channel.getExitStatus());
                    break;
                }
                try{Thread.sleep(100);}catch(Exception ee) {ee.printStackTrace();}
            }
            channel.disconnect();
            session.disconnect();
        }
        catch(Throwable e){
            e.printStackTrace();
            System.exit(1);
        }

        return result;
    }

    // Execute a remote SSH command on the specified host.
    public String cmd(String hostname, String command) {
        return cmdSSH(m_username, m_password, hostname, command);
    }

    // Execute a remote SSH command on the specified host.
    public String cmd(String hostname, String[] command) {
        return cmdSSH(m_username, m_password, hostname, command);
    }

    public ProcessData long_running_command(String hostname, String command[], String processName, OutputHandler handler) {
        return command(hostname, command, processName, handler);
    }

    public static void main(String args[]) throws Exception {
        SSHTools tools = new SSHTools(args[0], args[1]);
        System.out.println(tools.cmd( args[2], args[3]));
    }
}
