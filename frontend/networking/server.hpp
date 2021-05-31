#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <unistd.h>

#include <thread>
#include <unordered_map>
#include <vector>

namespace Net
{
template <class Parser>
class Server
{
  public:
   Server() { pthread_rwlock_init(&connLatch, NULL); };
   ~Server() { pthread_rwlock_destroy(&connLatch); };

   void init(uint16_t port)
   {
      // epoll instance
      if ((epfd = epoll_create(256)) == -1) {
         perror("epoll_create()");
         exit(EXIT_FAILURE);
      }

      // create socket
      if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
         perror("socket()");
         exit(EXIT_FAILURE);
      }
      setNonBlocking(listenfd);

      // allow immediate address reuse on restart
      // NOTE: tcp packets from a previous run could still be pending
      int enable = 1;
      if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1) {
         perror("setsockopt()");
         exit(EXIT_FAILURE);
      }

      // register listening socket
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.data.fd = listenfd;
      ev.events = EPOLLIN | EPOLLEXCLUSIVE;

      if (epoll_ctl(epfd, EPOLL_CTL_ADD, listenfd, &ev) == -1) {
         perror("epoll_ctl()");
         exit(EXIT_FAILURE);
      }

      // bind and listen
      struct sockaddr_in serveraddr;
      serveraddr.sin_family = AF_INET;
      serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
      serveraddr.sin_port = htons(port);
      if (bind(listenfd, (struct sockaddr*)&serveraddr, sizeof(serveraddr)) == -1) {
         perror("bind()");
         exit(EXIT_FAILURE);
      }
      setNonBlocking(listenfd);
      if (listen(listenfd, 100) == -1) {
         perror("listen()");
         exit(EXIT_FAILURE);
      }
   }

   template <typename Func>
   void runThread(std::atomic<bool>& keepRunning, Func callback)
   {
      // main loop
      struct epoll_event ev, events[1];
      Connection* connection;
      char readBuffer[Parser::maxPacketSize];

      while (keepRunning) {
         // wait for epoll events
         int nfds;
         if ((nfds = epoll_wait(epfd, events, 1, 100)) == 1000) {
            perror("epoll_wait()");
            exit(EXIT_FAILURE);
         }

         // event loop
         for (int i = 0; i < nfds; ++i) {
            ev = events[i];
            if (ev.data.fd == listenfd) {
               // new socket user detected, accept connection
               struct sockaddr_in clientaddr;
               socklen_t clilen = sizeof(clientaddr);
               int connfd = accept(listenfd, (struct sockaddr*)&clientaddr, &clilen);
               if (connfd == -1) {
                  if (errno == EAGAIN || errno == EWOULDBLOCK)
                     continue;

                  perror("accept()");
                  exit(EXIT_FAILURE);
               }

               // add connection to hashtable and epoll
               pthread_rwlock_wrlock(&connLatch);
               connections.try_emplace(connfd, connfd);
               pthread_rwlock_unlock(&connLatch);

               setNonBlocking(connfd);
               ev.data.fd = connfd;
               ev.events = EPOLLIN | EPOLLET | EPOLLONESHOT;
               if (epoll_ctl(epfd, EPOLL_CTL_ADD, connfd, &ev) == -1) {
                  perror("epoll_ctl()");
                  exit(EXIT_FAILURE);
               }
            } else if ((ev.events & EPOLLERR) || (ev.events & EPOLLHUP)) {
               // client closed connection
               closeConnection(ev.data.fd);
            } else {
               pthread_rwlock_rdlock(&connLatch);
               connection = &connections.at(ev.data.fd);
               pthread_rwlock_unlock(&connLatch);

               if (ev.events & EPOLLOUT) {
                  // write from outBuffer
                  auto& buf = connection->outBuffer;

                  while (keepRunning) {
                     auto n = send(ev.data.fd, &buf[0], buf.size(), MSG_NOSIGNAL);
                     if (n == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                           // socket buffer full
                           break;
                        } else if (errno == ECONNRESET) {
                           // client closed connection
                           closeConnection(ev.data.fd);
                           // continue with the event loop
                           goto continue_event_loop;
                        } else {
                           perror("send()");
                           exit(EXIT_FAILURE);
                        }
                     } else if (static_cast<size_t>(n) != buf.size()) {
                        // there's still some of the message left
                        buf.erase(buf.begin(), buf.begin() + n);
                        connection->epollEvents |= EPOLLOUT;
                     } else {
                        // complete message has been sent
                        buf.clear();
                        connection->epollEvents &= ~EPOLLOUT;
                        break;
                     }
                  }
               }

               if (ev.events & EPOLLIN) {
                  // read all data from socket until EAGAIN
                  while (keepRunning) {
                     ssize_t n = read(ev.data.fd, readBuffer, Parser::maxPacketSize);
                     if (n == -1) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) {
                           // all data read
                           break;
                        } else if (errno == ECONNRESET) {
                           // client closed connection
                           closeConnection(ev.data.fd);
                           // continue with the event loop
                           goto continue_event_loop;
                        } else {
                           perror("read()");
                           exit(EXIT_FAILURE);
                        }
                     } else if (n == 0) {
                        // client closed connection
                        closeConnection(ev.data.fd);
                        // continue with the event loop
                        goto continue_event_loop;
                     } else {
                        // forward readBuffer to packet protocol handler
                        connection->parser.parse(reinterpret_cast<uint8_t*>(readBuffer), n, callback);
                     }
                  }
               }

               // rearm socket
               ev.events = connection->epollEvents;
               if (epoll_ctl(epfd, EPOLL_CTL_MOD, ev.data.fd, &ev) == -1) {
                  perror("epoll_ctl()");
                  exit(EXIT_FAILURE);
               }
            }

         continue_event_loop:;
         }
      }
   };

  private:
   struct Connection {
      Connection(int fd) : fd(fd){};

      std::vector<uint8_t> outBuffer;
      Parser parser;
      uint32_t epollEvents = EPOLLIN | EPOLLET | EPOLLONESHOT;
      int fd;
   };

   int epfd;
   int listenfd;
   std::vector<std::thread> threads;
   std::unordered_map<int, Connection> connections;
   pthread_rwlock_t connLatch;

   void setNonBlocking(int socket)
   {
      int flags = fcntl(socket, F_GETFL);
      if (flags == -1) {
         perror("fcntl(GETFL)");
         exit(EXIT_FAILURE);
      }
      flags = flags | O_NONBLOCK;
      if (fcntl(socket, F_SETFL, flags) == -1) {
         perror("fcntl(SETFL)");
         exit(EXIT_FAILURE);
      }
   };

   void closeConnection(int fd)
   {
      pthread_rwlock_wrlock(&connLatch);
      connections.erase(fd);
      pthread_rwlock_unlock(&connLatch);
      close(fd);
   }
};
}  // namespace Net