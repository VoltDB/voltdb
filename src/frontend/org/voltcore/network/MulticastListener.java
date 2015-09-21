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

package org.voltcore.network;

import java.io.*;
import java.net.*;
import org.voltcore.logging.VoltLogger;


public class MulticastListener implements Runnable {

    VoltLogger log = new VoltLogger("MulticastClient");

    boolean m_terminate = false;
    public DatagramSocket m_socket;
    public InetAddress m_group;

    public void MulticastListener() throws Exception {
        //this.setName("MulticastClient");
        //this.setDaemon(true);
        m_socket = new DatagramSocket(14446);
    }

    public void run() {
        System.out.println("Voltdb database id is 'foobar'");

        DatagramPacket packet;
        while (!m_terminate) {
            byte[] buf = new byte[256];
            packet = new DatagramPacket(buf, buf.length);
            try {
                m_socket.receive(packet);
            } catch (Exception e) {
                log.error("dg receive failed", e);
            }
            String received = new String(packet.getData(), 0, packet.getLength());
            System.out.println("Received: " + received);
            try {
                Thread.sleep((long) 1*1000);
            } catch (InterruptedException e) {}
            }
        }

    public void shutdown() {
        m_terminate = true;
        //this.interrupt();
    }

    public void query() throws Exception {
        String dString = null;
        dString = "foobar";
        byte[] buf = new byte[256];
        buf = dString.getBytes();
        InetAddress address = InetAddress.getByName("10.10.183.58");
        System.out.println(address.toString());
        DatagramPacket packet;
        packet = new DatagramPacket(buf, buf.length, address, 14446);
        System.out.println("sending...");
        DatagramSocket socket = new DatagramSocket(14447);
        assert socket != null;
        assert packet != null;
        try {
            socket.send(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
