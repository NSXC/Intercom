#include <iostream>
#include <map>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include <fstream>
#include <winsock2.h>

#pragma comment(lib, "ws2_32.lib")

const int PORT = 8080;
const int MAX_CLIENTS = 10;

std::map<std::string, std::vector<std::pair<SOCKET, std::string>>> queues;
std::map<std::string, std::set<SOCKET>> subscribers;
std::map<std::string, std::vector<std::string>> persistentStorage;
std::map<std::string, std::vector<std::string>> deadLetterQueues;

void clientHandler(SOCKET clientSocket) {
    char buffer[1024];
    int bytesRead;

    while (true) {
        bytesRead = recv(clientSocket, buffer, sizeof(buffer), 0);
        if (bytesRead <= 0) {
            closesocket(clientSocket);
            return;
        }

        std::string message(buffer, bytesRead);
        std::string action;
        std::string data;

        size_t colonPos = message.find(':');
        if (colonPos != std::string::npos) {
            action = message.substr(0, colonPos);
            data = message.substr(colonPos + 1);
        }

        if (action == "SUBSCRIBE") {
            subscribers[data].insert(clientSocket);
        } else if (action == "UNSUBSCRIBE") {
            subscribers[data].erase(clientSocket);
        } else if (action == "PUBLISH") {
            size_t colonPos = data.find(':');
            if (colonPos != std::string::npos) {
                std::string actionName = data.substr(0, colonPos);
                std::string messageBody = data.substr(colonPos + 1);

                queues[actionName].push_back({clientSocket, messageBody});
                persistentStorage[actionName].push_back(messageBody);
            }
        } else if (action == "CONSUME") {
            if (queues[data].empty()) {
                send(clientSocket, "QUEUE_EMPTY", 11, 0);
            } else {
                std::pair<SOCKET, std::string> message = queues[data].front();
                queues[data].pop_back();
                send(clientSocket, message.second.c_str(), message.second.length(), 0);
            }
        } else if (action == "ACKNOWLEDGE") {
            std::string actionName = data;
            auto it = std::find_if(persistentStorage[actionName].begin(), persistentStorage[actionName].end(),
                [&clientSocket](const std::string& msg) {
                    return msg.find("ACK:" + std::to_string(clientSocket)) == 0;
                });
            if (it != persistentStorage[actionName].end()) {
                persistentStorage[actionName].erase(it);
            }
        } else if (action == "REQUEUE") {
            std::string actionName = data;
            if (!deadLetterQueues[actionName].empty()) {
                std::string message = deadLetterQueues[actionName].front();
                queues[actionName].push_back({clientSocket, message});
                deadLetterQueues[actionName].pop_back();
            }
        }
    }
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "Failed to initialize Winsock." << std::endl;
        return 1;
    }

    SOCKET serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == INVALID_SOCKET) {
        std::cerr << "Failed to create server socket." << std::endl;
        WSACleanup();
        return 1;
    }

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Failed to bind server socket." << std::endl;
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    if (listen(serverSocket, MAX_CLIENTS) == SOCKET_ERROR) {
        std::cerr << "Failed to listen on server socket." << std::endl;
        closesocket(serverSocket);
        WSACleanup();
        return 1;
    }

    std::cout << "Server is listening on port " << PORT << std::endl;

    while (true) {
        SOCKET clientSocket = accept(serverSocket, NULL, NULL);
        if (clientSocket == INVALID_SOCKET) {
            std::cerr << "Failed to accept client connection." << std::endl;
            closesocket(serverSocket);
            WSACleanup();
            return 1;
        }

        std::thread(clientHandler, clientSocket).detach();
    }

    closesocket(serverSocket);
    WSACleanup();

    return 0;
}
