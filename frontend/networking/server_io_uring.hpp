#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <thread>
#include <unordered_map>
#include <vector>

#include "OBuffer.hpp"

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
      // init uring
      constexpr uint QUEUE_DEPTH = 256;
      io_uring_queue_init(QUEUE_DEPTH, &ring, 0);

      // create socket
      if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
         perror("socket()");
         exit(EXIT_FAILURE);
      }

      // allow immediate address reuse on restart
      // NOTE: tcp packets from a previous run could still be pending
      int enable = 1;
      if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1) {
         perror("setsockopt(SO_REUSEADDR)");
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
      if (listen(listenfd, 100) == -1) {
         perror("listen()");
         exit(EXIT_FAILURE);
      }
   }

   template <typename Func>
   void runThread(std::atomic<bool>& keepRunning, Func callback)
   {
      struct io_uring_cqe* cqe;
      struct sockaddr_in clientaddr;
      socklen_t clilen = sizeof(clientaddr);

      addAcceptRequest(listenfd, &clientaddr, &clilen);

      while (keepRunning) {
         int ret = io_uring_wait_cqe(&ring, &cqe);
         struct Connection* con = (struct Connection*)cqe->user_data;
         if (ret < 0) {
            perror("io_uring_wait_cqe()");
            exit(EXIT_FAILURE);
         }
         if (cqe->res < 0) {
            fprintf(stderr, "Async request failed: %s for event: %d\n", strerror(-cqe->res), static_cast<int>(con->eventType));
            exit(EXIT_FAILURE);
         }

         switch (con->eventType) {
            case EventType::accept:
               con->fd = cqe->res;
               addReadRequest(con);
               addAcceptRequest(listenfd, &clientaddr, &clilen);
               break;
            case EventType::read:
               if (cqe->res == 0) {
                  // close connection
                  close(con->fd);
                  delete con;
               }
               con->parser.parse(cqe->res, callback);
               addWriteRequest(con);
               break;
            case EventType::write:
               con->oBuffer.pop_front(cqe->res);
               if (!con->oBuffer.empty()) {
                  addWriteRequest(con);
               } else {
                  addReadRequest(con);
               }
               break;
         }
         // mark this request as processed
         io_uring_cqe_seen(&ring, cqe);
      }
   }

  private:
   enum class EventType : int { accept = 0, read = 1, write = 2 };

   struct Connection {
      OBuffer<uint8_t> oBuffer;
      Parser parser{oBuffer};
      EventType eventType;
      int fd;
   };

   struct io_uring ring;
   int listenfd;
   std::vector<std::thread> threads;
   std::unordered_map<int, Connection> connections;
   pthread_rwlock_t connLatch;

   void addAcceptRequest(int server_socket, struct sockaddr_in* client_addr, socklen_t* client_addr_len)
   {
      struct Connection* con = new Connection();
      con->eventType = EventType::accept;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_accept(sqe, server_socket, (struct sockaddr*)client_addr, client_addr_len, 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }

   void addReadRequest(Connection* con)
   {
      con->eventType = EventType::read;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_read(sqe, con->fd, con->parser.getReadBuffer(), Parser::maxPacketSize, 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }

   void addWriteRequest(Connection* con)
   {
      con->eventType = EventType::write;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_write(sqe, con->fd, &(con->oBuffer.buffer)[0], con->oBuffer.buffer.size(), 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }
};
}  // namespace Net