/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

#include <iostream>
#include <string>
#include <vector>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <set>
#include <deque>
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <queue>
#include <limits>

using namespace std;
using boost::asio::ip::tcp;

class QueueEntry {
public:
    QueueEntry(tcp::socket *nsocket, int64_t nrequestId) : socket(nsocket), requestId(nrequestId) {}
    tcp::socket *socket;
    int64_t requestId;
};

class CompareQueueEntry {
public:
    bool operator()(const QueueEntry& lhs, const QueueEntry& rhs) const {
        if (lhs.requestId > rhs.requestId) {
            return true;
        }
        return false;
    }
};

class WriteStuff {
public:
    WriteStuff(size_t nlength, size_t noffset, int64_t *ncounter) : length(nlength), offset(noffset), counter(ncounter) {}
    size_t length;
    size_t offset;
    int64_t *counter;
};

class Server
 {
public:
    Server(boost::asio::io_service &io_service, std::vector<std::string> servers) :
        nextRequestId(0),
        requestsReceived(0),
        responsesSent(0),
        meshRequestsReceived(0),
        meshRequestsSent(0),
        meshResponsesSent(0),
        meshResponsesReceived(0),
         bytesWritten(0),
         bytesRead(0),
         bytesWrittenLastTime(0),
         bytesReadLastTime(0),
        serverAcceptor(io_service, tcp::endpoint(tcp::v4(), 21413)),
        clientAcceptor(io_service, tcp::endpoint(tcp::v4(), 21412)) {
        tcp::socket *socket = new tcp::socket(io_service);
        serverAcceptor.async_accept( *socket,
                boost::bind(
                        &Server::handleServerAccept,
                        this,
                        boost::asio::placeholders::error,
                        &serverAcceptor,
                        socket));


        socket = new tcp::socket(io_service);
        clientAcceptor.async_accept( *socket,
                boost::bind(
                        &Server::handleClientAccept,
                        this,
                        boost::asio::placeholders::error,
                        &clientAcceptor,
                        socket));

        tcp::resolver resolver(io_service);
        for (std::size_t ii = 0; ii < servers.size(); ii++) {
            tcp::resolver::query query(servers[ii], "21413");
            tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
            tcp::socket* socket = new tcp::socket(io_service);
            tcp::endpoint endpoint = *endpoint_iterator;
            socket->async_connect( endpoint,
                      boost::bind( &Server::handleConnect, this, boost::asio::placeholders::error, endpoint_iterator, socket));
        }

        boost::asio::deadline_timer *timer = new boost::asio::deadline_timer(io_service);
        timer->expires_from_now(boost::posix_time::seconds(10));
        timer->async_wait(boost::bind(&Server::timerCallback, this, boost::asio::placeholders::error, timer));
    }

    void timerCallback(const boost::system::error_code& error, boost::asio::deadline_timer *timer) {
        if (error) {
            std::cout << error.message() << std::endl;
        }
        int64_t bytesReadThisTime = bytesRead - bytesReadLastTime;
        bytesReadLastTime = bytesRead;
        int64_t bytesWrittenThisTime = bytesWritten - bytesWrittenLastTime;
        bytesWrittenLastTime = bytesWritten;
        int64_t mbReadThisTime = bytesReadThisTime / (1024 * 1024);
        int64_t mbWrittenThisTime = bytesWrittenThisTime / (1024 * 1024);
        double mbReadPerSec = mbReadThisTime / 10.0;
        double mbWrittenPerSec = mbWrittenThisTime / 10.0;
        std::cout << "Megabytes/sec In/Out " << mbReadPerSec << "/" << mbWrittenPerSec << std::endl;
        std::cout << "Requests received " << requestsReceived << " Responses sent " << responsesSent << " MRequests received "
                << meshRequestsReceived << " MRequests sent " << meshRequestsSent << " MResponses sent " << meshResponsesSent <<
                " MResponses received " << meshResponsesReceived << " Outstanding requests " << (requestsReceived - responsesSent) << std::endl;
        timer->expires_from_now(boost::posix_time::seconds(10));
        timer->async_wait(boost::bind(&Server::timerCallback, this, boost::asio::placeholders::error, timer));
    }

    void handleConnect(const boost::system::error_code& error,
            tcp::resolver::iterator endpoint_iterator, tcp::socket* socket) {
        if (error) {
            std::cout << error.message() << std::endl;
            socket->close();
            ++endpoint_iterator;
            if (endpoint_iterator != tcp::resolver::iterator()) {
                tcp::endpoint endpoint = *endpoint_iterator;
                socket->async_connect(endpoint,
                        boost::bind( &Server::handleConnect, this, boost::asio::placeholders::error, endpoint_iterator, socket));
            }
        } else {
            std::cout << "Connected to server " << socket->remote_endpoint().address().to_string() << std::endl;
            boost::asio::ip::tcp::no_delay option(true);
            socket->set_option(option);
            boost::asio::ip::tcp::socket::non_blocking_io nonblocking(true);
            socket->io_control(nonblocking);
            serverSockets.push_back(socket);
            socketAsyncWritePending[socket] = false;
            char *buffer = acquireBuffer();
            requestIdsResponded[socket] = new boost::unordered_set<int64_t>();
            requestIdsRequested[socket] = new boost::unordered_set<int64_t>();
            socketAsyncWrites[socket] = new std::deque<std::pair<char*, WriteStuff> >();
            boost::asio::async_read( *socket, boost::asio::buffer( buffer, 4), boost::bind(&Server::handleServerRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, true));
        }
    }

    void handleServerAccept(
            const boost::system::error_code& error,
            tcp::acceptor *acceptor,
            tcp::socket* socket) {
        if (error) {
            std::cout << error.message() << std::endl;
            return;
        }
        if (!error) {
            std::cout << "Accepted from server " << socket->remote_endpoint().address().to_string() << std::endl;
            socketAsyncWritePending[socket] = false;
            serverSockets.push_back(socket);
            tcp::socket *newsocket = new tcp::socket(acceptor->io_service());
            acceptor->async_accept(*newsocket, boost::bind(&Server::handleServerAccept, this, boost::asio::placeholders::error, acceptor, newsocket));
            boost::asio::ip::tcp::no_delay option(true);
            socket->set_option(option);
            boost::asio::ip::tcp::socket::receive_buffer_size receiveSize(262144);
            boost::asio::ip::tcp::socket::send_buffer_size sendSize(262144);
            socket->set_option(receiveSize);
            socket->set_option(sendSize);
            char *buffer = acquireBuffer();
            boost::asio::ip::tcp::socket::non_blocking_io nonblocking(true);
            socket->io_control(nonblocking);
            requestIdsResponded[socket] = new boost::unordered_set<int64_t>();
            requestIdsRequested[socket] = new boost::unordered_set<int64_t>();
            socketAsyncWrites[socket] = new std::deque<std::pair<char*, WriteStuff> >();
            boost::asio::async_read( *socket, boost::asio::buffer( buffer, 4), boost::bind(&Server::handleServerRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, true));
        }
    }

    void handleServerRead(const boost::system::error_code& error,
            size_t bytes_transferred, tcp::socket* socket, char *buffer, bool lengthOrMessage) {
        if (error) {
            std::cout << error.message() << std::endl;
            releaseBuffer(buffer);
            return;
        }

        bytesRead += bytes_transferred;

        size_t nextLength = 0;
        size_t nextBufferOffset;
        while (true) {
            nextBufferOffset = 0;
            if (lengthOrMessage) {
                nextLength = *reinterpret_cast<int32_t*>(buffer);
                nextBufferOffset = 4;
                lengthOrMessage = false;
                if (nextLength <= 0 || nextLength > 1500) {
                    std::cout << "Next length from server was " << nextLength << std::endl;
                    exit(-1);
                }
            } else {
                nextLength = 4;
                lengthOrMessage = true;
                if (buffer[4] == 0) {
                    //request
                    meshRequestsReceived++;
                    int64_t requestId = *reinterpret_cast<int64_t*>(&buffer[5]);
                    addQueueEntry(socket, requestId);
                } else if (buffer[4] == 1) {
                    //response
                    meshResponsesReceived++;
                    int32_t responseLength = *reinterpret_cast<int32_t*>(buffer);
                    int64_t requestId = *reinterpret_cast<int64_t*>(&buffer[5]);
                    char *copy = acquireBuffer();
                    ::memcpy( copy, buffer, responseLength + 4);
                    tcp::socket *clientSocket = requestIdToClient[requestId];
                    requestIdToClient.erase(requestId);
//                    if (!requestIdsResponded[socket]->insert(requestId).second) {
//                        std::cout << "Request ID " << requestId << " has already been responded to" << std::endl;
//                    }

                    size_t written = 0;
                    bool asyncWritePending = socketAsyncWritePending[clientSocket];
                    try {
                        if (!asyncWritePending) {
                            written = clientSocket->write_some(boost::asio::buffer(copy, responseLength + 4));
                        }
                    } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
                    }

                    bytesWritten += written;
                    if (written != static_cast<size_t>(responseLength + 4)) {
                        size_t length = responseLength + 4 - written;
                        if (asyncWritePending) {
                            socketAsyncWrites[clientSocket]->push_back(std::pair<char*, WriteStuff>(copy, WriteStuff(length, written, &responsesSent)));
                        } else {
                            boost::asio::async_write( *clientSocket, boost::asio::buffer(&copy[written], length),
                                    boost::bind(&Server::handleWriteCompletion, this, boost::asio::placeholders::error,
                                            boost::asio::placeholders::bytes_transferred, clientSocket, copy, &responsesSent));
                            socketAsyncWritePending[clientSocket] = true;
                        }
                    } else {
                        releaseBuffer(copy);
                        responsesSent++;
                        backpressureCheck();
                    }
                }
            }

            try {
                size_t read = socket->read_some(boost::asio::buffer( &buffer[nextBufferOffset], nextLength));
                bytesRead += read;
                if (read < nextLength) {
                    boost::asio::async_read( *socket, boost::asio::buffer(&buffer[nextBufferOffset + read], nextLength - read),
                            boost::bind(&Server::handleServerRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                    drainPriorityQueue();
                    return;
                }
            } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
                boost::asio::async_read( *socket, boost::asio::buffer(&buffer[nextBufferOffset], nextLength),
                        boost::bind(&Server::handleServerRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                drainPriorityQueue();
                return;
            }
        }
    }

    void handleClientRead(const boost::system::error_code& error,
            size_t bytes_transferred, tcp::socket* socket, char *buffer, bool lengthOrMessage) {
        if (error) {
            std::cout << error.message() << std::endl;
            releaseBuffer(buffer);
            return;
        }
        bytesRead += bytes_transferred;

        const size_t maxRead = 16384;
        size_t totalClientRead = 0;
        size_t nextLength = 0;
        int outstandingRequests = 0;
        while (true) {
            if (lengthOrMessage) {
                nextLength = *reinterpret_cast<int32_t*>(buffer);
                lengthOrMessage = false;
                if (nextLength <= 0 || nextLength > 1500) {
                    std::cout << "Next length from client was " << nextLength << std::endl;
                    exit(-1);
                }
            } else {
                nextLength = 4;
                lengthOrMessage = true;
                requestsReceived++;
                int64_t requestId = nextRequestId++;
                char *request = acquireBuffer();
                *reinterpret_cast<int32_t*>(request) = 60;
                *reinterpret_cast<int8_t*>(&request[4]) = 0;//request
                *reinterpret_cast<int64_t*>(&request[5]) = requestId;
                requestIdToClient[requestId] = socket;

                size_t serverIndex = rand() % serverSockets.size();
                tcp::socket *serverSocket = serverSockets[serverIndex];

                size_t written = 0;
                bool asyncWritePending = socketAsyncWritePending[serverSocket];
                try {
                    if ( !asyncWritePending ) {
                        written = serverSocket->write_some(boost::asio::buffer(request, 64));
                    }
                } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
                }

                bytesWritten += written;
                if (written < 64) {
                    size_t length = 64 - written;
                    if (asyncWritePending) {
                        socketAsyncWrites[serverSocket]->push_back(std::pair<char*, WriteStuff>(request, WriteStuff(length, written, &meshRequestsSent)));
                    } else {
                        boost::asio::async_write( *serverSocket, boost::asio::buffer(&request[written], length),
                                boost::bind(&Server::handleWriteCompletion, this, boost::asio::placeholders::error,
                                        boost::asio::placeholders::bytes_transferred, serverSocket, request, &meshRequestsSent));
                        socketAsyncWritePending[serverSocket] = true;
                    }
                } else {
                    releaseBuffer(request);
                    meshRequestsSent++;
                }
            }

            outstandingRequests = requestsReceived - responsesSent;
            if (outstandingRequests > 15000 && lengthOrMessage) {
                releaseBuffer(buffer);
                backpressureSockets.insert(socket);
                return;
            } else {
                try {
                    size_t read = 0;
                    if (lengthOrMessage || totalClientRead < maxRead) {
                        read = socket->read_some(boost::asio::buffer(buffer, nextLength));
                        bytesRead += read;
                        totalClientRead += read;
                    }
                    if (read < nextLength) {
                        boost::asio::async_read( *socket, boost::asio::buffer(&buffer[read], nextLength - read),
                                                            boost::bind(&Server::handleClientRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                        return;
                    }
                } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
                    boost::asio::async_read( *socket, boost::asio::buffer(buffer, nextLength),
                                                        boost::bind(&Server::handleClientRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                    return;
                }
            }
        }
    }

    void handleWriteCompletion(const boost::system::error_code& error,
            size_t bytes_transferred, tcp::socket* socket, char *buffer, int64_t *counter) {
        if (error) {
            std::cout << error.message() << std::endl;
            releaseBuffer(buffer);
            return;
        }
        bytesWritten += bytes_transferred;
        *counter = *counter + 1;
        releaseBuffer(buffer);
        socketAsyncWritePending[socket] = false;
        backpressureCheck();
        std::deque<std::pair<char*, WriteStuff> > *pendingWrites = socketAsyncWrites[socket];
        while (!pendingWrites->empty()) {
            std::pair<char*, WriteStuff> pendingWrite = pendingWrites->front();
            pendingWrites->pop_front();
            char * pendingBuffer = pendingWrite.first;
            size_t writeLength = pendingWrite.second.length;
            size_t writeOffset = pendingWrite.second.offset;
            int64_t *counter = pendingWrite.second.counter;
            size_t written = 0;
            try {
                written = socket->write_some(boost::asio::buffer( &pendingBuffer[writeOffset], writeLength));
            } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
            }

            if (written < writeLength) {
                size_t length = writeLength - written;
                boost::asio::async_write( *socket, boost::asio::buffer(&pendingBuffer[writeOffset + written], length),
                        boost::bind(&Server::handleWriteCompletion, this, boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred, socket, pendingBuffer, counter));
                socketAsyncWritePending[socket] = true;
                break;
            } else {
                releaseBuffer(pendingBuffer);
                *counter = *counter + 1;
            }
        }
    }

    /*
     * Check if there is no more backpressure and reactivate reads for client sockets.
     */
    void backpressureCheck() {
        int outstandingRequests = requestsReceived - responsesSent;
        if (outstandingRequests < 10000) {
            if (backpressureSockets.size() > 0) {
                for (boost::unordered_set<tcp::socket*>::iterator i = backpressureSockets.begin();
                        i != backpressureSockets.end(); i++) {
                    tcp::socket *clientSocket = *i;
                    char *newbuffer = acquireBuffer();
                    boost::asio::async_read( *clientSocket, boost::asio::buffer( newbuffer, 4), boost::bind(&Server::handleClientRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, clientSocket, newbuffer, true));
                }
                backpressureSockets.clear();
            }
        }
    }

    void handleClientAccept(
            const boost::system::error_code& error,
            tcp::acceptor *acceptor,
            tcp::socket* socket) {
        if (error) {
            std::cout << error.message() << std::endl;
            return;
        }
        if (!error) {
            std::cout << "Accepted from client " << socket->remote_endpoint().address().to_string() << std::endl;
            clientSockets.push_back(socket);
            tcp::socket *newsocket = new tcp::socket(acceptor->io_service());
            acceptor->async_accept(*newsocket, boost::bind(&Server::handleClientAccept, this, boost::asio::placeholders::error, acceptor, newsocket));
            char *buffer = acquireBuffer();
            boost::asio::ip::tcp::socket::receive_buffer_size receiveSize(262144);
            boost::asio::ip::tcp::socket::send_buffer_size sendSize(262144);
            boost::asio::ip::tcp::socket::non_blocking_io nonblocking(true);
            socket->io_control(nonblocking);
            socket->set_option(receiveSize);
            socket->set_option(sendSize);
            socketAsyncWritePending[socket] = false;
            socketAsyncWrites[socket] = new std::deque<std::pair<char*, WriteStuff> >();
            boost::asio::async_read( *socket, boost::asio::buffer( buffer, 4), boost::bind(&Server::handleClientRead, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, true));
        }
    }

    char* acquireBuffer() {
        if (buffers.empty()) {
            char * buffer = new char[1500];
            ::memset(buffer, 0, 1500);
            return buffer;
        } else {
            char *buffer = buffers.back();
            buffers.pop_back();
            return buffer;
        }
    }

    void releaseBuffer(char *buffer) {
        buffers.push_back(buffer);
    }

    void addQueueEntry(tcp::socket *socket, int64_t requestId) {
        std::map<tcp::socket*, int64_t>::iterator iter = lastSafeRequestId.find(socket);
        if (iter == lastSafeRequestId.end()) {
            lastSafeRequestId[socket] = requestId;
        } else if (iter->second < requestId) {
            iter->second = requestId;
        }
        queue.push(QueueEntry(socket, requestId));
    }

    void sendResponse(QueueEntry entry) {
        tcp::socket *socket = entry.socket;

        char *response = acquireBuffer();
        int responseLength = 256 + (rand() % 600);
        ::memset(response, 0, responseLength + 4);
        *reinterpret_cast<int32_t*>(response) = responseLength;
        *reinterpret_cast<int8_t*>(&response[4]) = 1;//response
        *reinterpret_cast<int64_t*>(&response[5]) = entry.requestId;
//                    if (!requestIdsRequested[socket]->insert(*reinterpret_cast<int64_t*>(&response[5])).second) {
//                        std::cout << "Request ID " << *reinterpret_cast<int64_t*>(&response[5]) << " has already been requested" << std::endl;
//                    }
        size_t written = 0;
        bool asyncWritePending = socketAsyncWritePending[socket];
        try {
            if (!asyncWritePending) {
                written = socket->write_some(boost::asio::buffer(response, responseLength + 4));
            }
        } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
        }

        bytesWritten += written;
        if (written != static_cast<size_t>(responseLength + 4)) {
            size_t length = responseLength + 4 - written;
            if (asyncWritePending) {
                socketAsyncWrites[socket]->push_back(std::pair<char*, WriteStuff>(response, WriteStuff(length, written, &meshResponsesSent)));
            } else {
                boost::asio::async_write( *socket, boost::asio::buffer(&response[written], length),
                        boost::bind(&Server::handleWriteCompletion, this, boost::asio::placeholders::error,
                                boost::asio::placeholders::bytes_transferred, socket, response, &meshResponsesSent));
                socketAsyncWritePending[socket] = true;
            }
        } else {
            releaseBuffer(response);
            meshResponsesSent++;
        }
    }

    void drainPriorityQueue() {
        int64_t minRequestId = std::numeric_limits<int64_t>::max();
        for (std::map<tcp::socket*, int64_t>::iterator iter = lastSafeRequestId.begin();
                iter != lastSafeRequestId.end();
                iter++) {
            if (iter->second < minRequestId) {
                minRequestId = iter->second;
            }
        }
        while (!queue.empty() && queue.top().requestId <= minRequestId) {
            QueueEntry entry = queue.top();
            queue.pop();
            sendResponse(entry);
        }
    }

    std::deque<char*> buffers;
    int64_t nextRequestId;
    int64_t requestsReceived;
    int64_t responsesSent;
    int64_t meshRequestsReceived;
    int64_t meshRequestsSent;
    int64_t meshResponsesSent;
    int64_t meshResponsesReceived;
    int64_t bytesWritten;
    int64_t bytesRead;
    int64_t bytesWrittenLastTime;
    int64_t bytesReadLastTime;
    boost::unordered_set<tcp::socket*> backpressureSockets;
    tcp::acceptor serverAcceptor;
    tcp::acceptor clientAcceptor;
    std::vector<tcp::socket*> serverSockets;
    std::vector<tcp::socket*> clientSockets;
    boost::unordered_map<tcp::socket*, boost::unordered_set<int64_t>*> requestIdsResponded;
    boost::unordered_map<tcp::socket*, boost::unordered_set<int64_t>*> requestIdsRequested;
    boost::unordered_map<int64_t, tcp::socket*> requestIdToClient;
    boost::unordered_map<tcp::socket*, bool> socketAsyncWritePending;
    boost::unordered_map<tcp::socket*, std::deque<std::pair<char*, WriteStuff> >* > socketAsyncWrites;
    std::map<tcp::socket*, int64_t> lastSafeRequestId;
    std::priority_queue< QueueEntry, std::vector<QueueEntry>, CompareQueueEntry> queue;
};

class Client {
public:
    Client( boost::asio::io_service &io_service, std::vector<std::string> servers) :
        requestsSent(0),
        responsesReceived(0),
        responsesReceivedLastTime(0),
     bytesWritten(0),
     bytesRead(0),
     bytesWrittenLastTime(0),
     bytesReadLastTime(0) {
        tcp::resolver resolver(io_service);
        for (std::size_t ii = 0; ii < servers.size(); ii++) {
            tcp::resolver::query query(servers[ii], "21412");
            tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
            tcp::socket* socket = new tcp::socket(io_service);
            tcp::endpoint endpoint = *endpoint_iterator;
            socket->async_connect( endpoint,
                      boost::bind( &Client::handleConnect, this, boost::asio::placeholders::error, endpoint_iterator, socket));
        }
        boost::asio::deadline_timer *timer = new boost::asio::deadline_timer(io_service);
        timer->expires_from_now(boost::posix_time::seconds(10));
        timer->async_wait(boost::bind(&Client::timerCallback, this, boost::asio::placeholders::error, timer));
    }

    void timerCallback(const boost::system::error_code& error, boost::asio::deadline_timer *timer) {
        if (error) {
            std::cout << error.message() << std::endl;
        }
        int64_t bytesReadThisTime = bytesRead - bytesReadLastTime;
        bytesReadLastTime = bytesRead;
        int64_t bytesWrittenThisTime = bytesWritten - bytesWrittenLastTime;
        bytesWrittenLastTime = bytesWritten;
        int64_t mbReadThisTime = bytesReadThisTime / (1024 * 1024);
        int64_t mbWrittenThisTime = bytesWrittenThisTime / (1024 * 1024);
        double mbReadPerSec = mbReadThisTime / 10.0;
        double mbWrittenPerSec = mbWrittenThisTime / 10.0;
        int64_t responsesReceivedThisTime = responsesReceived - responsesReceivedLastTime;
        responsesReceivedLastTime = responsesReceived;
        double requestsPerSec = responsesReceivedThisTime / 10.0;
        std::cout << "Requests/sec " <<  requestsPerSec << " Requests sent " << requestsSent << " Responses received " << responsesReceived << " Megabytes/sec In/Out " << mbReadPerSec << "/" << mbWrittenPerSec << std::endl;
        timer->expires_from_now(boost::posix_time::seconds(10));
        timer->async_wait(boost::bind(&Client::timerCallback, this, boost::asio::placeholders::error, timer));
    }

    void handleConnect(const boost::system::error_code& error,
            tcp::resolver::iterator endpoint_iterator, tcp::socket* socket) {
        if (error) {
            std::cout << error.message() << std::endl;
            socket->close();
            ++endpoint_iterator;
            if (endpoint_iterator != tcp::resolver::iterator()) {
                tcp::endpoint endpoint = *endpoint_iterator;
                socket->async_connect(endpoint,
                        boost::bind( &Client::handleConnect, this, boost::asio::placeholders::error, endpoint_iterator, socket));
            }
        } else {
            std::cout << "Connected to " << socket->remote_endpoint().address().to_string() << std::endl;
            char *recBuffer = new char[1500];
            ::memset(recBuffer, 0, 1500);
            char *sendBuffer = new char[1500];
            ::memset(sendBuffer, 0, 1500);
            *reinterpret_cast<int32_t*>(sendBuffer) = 60;
            boost::asio::ip::tcp::socket::receive_buffer_size receiveSize(262144);
            boost::asio::ip::tcp::socket::send_buffer_size sendSize(262144);
            socket->set_option(receiveSize);
            socket->set_option(sendSize);
            boost::asio::ip::tcp::socket::non_blocking_io nonblocking(true);
            socket->io_control(nonblocking);
            boost::asio::async_write( *socket, boost::asio::buffer(sendBuffer, 64),
                    boost::bind(&Client::handleWriteCompletion, this, boost::asio::placeholders::error,
                            boost::asio::placeholders::bytes_transferred, socket, sendBuffer, 64));
            boost::asio::async_read( *socket, boost::asio::buffer(recBuffer, 4),
                    boost::bind(&Client::handleReadCompletion, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, recBuffer, true));
        }
    }

    void handleWriteCompletion(const boost::system::error_code& error,
            size_t bytes_transferred, tcp::socket* socket, char *buffer, size_t expectedWrite) {
        if(error) {
            std::cout << error.message() << std::endl;
            return;
        }

        if (bytes_transferred != expectedWrite) {
            std::cout << "Bytes transferred was not equal to expected write in client" << std::endl;
        }

        bytesWritten += bytes_transferred;
        requestsSent++;
        size_t written;
        while (true) {
            written = 0;
            try {
                written = socket->write_some(boost::asio::buffer(buffer, 64));
            } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
            }
            bytesWritten += written;
            if (written < 64) {
                break;
            }
            requestsSent++;
        }
        boost::asio::async_write( *socket, boost::asio::buffer(&buffer[written], 64 - written),
                boost::bind(&Client::handleWriteCompletion, this, boost::asio::placeholders::error,
                        boost::asio::placeholders::bytes_transferred, socket, buffer, 64 - written));
    }

    void handleReadCompletion(const boost::system::error_code& error,
            size_t bytes_transferred, tcp::socket* socket, char *buffer, bool lengthOrMessage) {
        if (error) {
            std::cout << error.message() << std::endl;
            return;
        }

        bytesRead += bytes_transferred;
        size_t currentLength = 0;

        while (true) {
            if (lengthOrMessage) {
                currentLength = *reinterpret_cast<int32_t*>(buffer);
                lengthOrMessage = false;
                if (currentLength <= 0 || currentLength > 1500) {
                    std::cout << "Next length at client was " << currentLength << std::endl;
                    exit(-1);
                }
            } else {
                responsesReceived++;
                currentLength = 4;
                lengthOrMessage = true;
            }

            try {
                size_t read = socket->read_some(boost::asio::buffer(buffer, currentLength));
                bytesRead += read;
                if (read < currentLength) {
                    boost::asio::async_read( *socket, boost::asio::buffer(&buffer[read], currentLength - read),
                                        boost::bind(&Client::handleReadCompletion, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                    return;
                }
            } catch (boost::exception_detail::clone_impl<boost::exception_detail::error_info_injector<boost::system::system_error> >) {
                boost::asio::async_read( *socket, boost::asio::buffer(buffer, currentLength),
                                    boost::bind(&Client::handleReadCompletion, this, boost::asio::placeholders::error, boost::asio::placeholders::bytes_transferred, socket, buffer, lengthOrMessage));
                return;
            }
        }
    }

    int64_t requestsSent;
    int64_t responsesReceived;
    int64_t responsesReceivedLastTime;
    int64_t bytesWritten;
    int64_t bytesRead;
    int64_t bytesWrittenLastTime;
    int64_t bytesReadLastTime;
};

int main(int argc, char **argv) {
    boost::asio::io_service io_service;
    if (std::string(argv[1]) == "server") {
        std::vector<std::string> machines;
        for (int ii = 2; ii < argc; ii++) {
            machines.push_back(std::string(argv[ii]));
        }
        Server server( io_service, machines);
        io_service.run();
    } else if (std::string(argv[1]) == "client") {
        std::vector<std::string> machines;
        for (int ii = 2; ii < argc; ii++) {
            machines.push_back( std::string(argv[ii]));
        }
        Client client( io_service, machines);
        io_service.run();
    }
    return 0;
}
