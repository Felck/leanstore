#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include <thread>
#include <vector>

#include "OBuffer.hpp"

namespace Net
{
template <class Parser>
class ServerIOUring
{
  public:
   ServerIOUring(){};
   ~ServerIOUring(){};

   void init(uint16_t port)
   {  // create socket
      if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
         perror("socket()");
         exit(EXIT_FAILURE);
      }

      // allow immediate address reuse on restart
      // NOTE: tcp packets from a previous run could still be pending
      int enable = 1;
      if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(enable)) == -1) {
         perror("setsockopt(SO_REUSEPORT)");
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
      // init uring
      struct io_uring ring;
      constexpr uint QUEUE_DEPTH = 256;
      struct io_uring_params params;
      memset(&params, 0, sizeof(params));

      if (uring_wq_fd == -1) {
         params.flags |= IORING_SETUP_SQPOLL;
         params.sq_thread_idle = 2000;
         uring_wq_fd = io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params);
         if (uring_wq_fd == -1) {
            perror("uring_queue_init()");
            exit(EXIT_FAILURE);
         }
      } else {
         params.flags |= IORING_SETUP_ATTACH_WQ | IORING_SETUP_SQPOLL;
         params.wq_fd = uring_wq_fd;
         params.sq_thread_idle = 2000;
         if (io_uring_queue_init_params(QUEUE_DEPTH, &ring, &params) == -1) {
            perror("uring_queue_init()");
            exit(EXIT_FAILURE);
         }
      }

      struct io_uring_cqe* cqe;
      struct sockaddr_in clientaddr;
      socklen_t clilen = sizeof(clientaddr);

      addAcceptRequest(ring, listenfd, &clientaddr, &clilen);

      // event loop
      while (keepRunning) {
         int ret = io_uring_wait_cqe(&ring, &cqe);
         if (ret < 0) {
            perror("io_uring_wait_cqe()");
            exit(EXIT_FAILURE);
         }
         struct Connection* con = (struct Connection*)cqe->user_data;

         switch (con->eventType) {
            case EventType::accept:
               if (cqe->res < 0) {
                  fprintf(stderr, "accept(): %s\n", strerror(-cqe->res));
                  exit(EXIT_FAILURE);
               }
               con->fd = cqe->res;
               addReadRequest(ring, con);
               addAcceptRequest(ring, listenfd, &clientaddr, &clilen);
               break;
            case EventType::read:
               if (cqe->res < 0) {
                  if (-cqe->res == ECONNRESET) {
                     // close connection
                     close(con->fd);
                     delete con;
                     break;
                  } else {
                     fprintf(stderr, "read(): %s\n", strerror(-cqe->res));
                     exit(EXIT_FAILURE);
                  }
               }
               if (cqe->res == 0) {
                  // close connection
                  close(con->fd);
                  delete con;
                  break;
               }
               con->parser.parse(cqe->res, callback);
               // if (con->parser.packetComplete())
               addWriteRequest(ring, con);  // optimization: another readRequest unless a complete packet was received
               // else
               //   addReadRequest(ring, con);

               break;
            case EventType::write:
               if (cqe->res < 0) {
                  if (-cqe->res == ECONNRESET) {
                     // close connection
                     close(con->fd);
                     delete con;
                     break;
                  } else {
                     fprintf(stderr, "write(): %s\n", strerror(-cqe->res));
                     exit(EXIT_FAILURE);
                  }
               }
               con->oBuffer.pop_front(cqe->res);
               if (!con->oBuffer.empty()) {
                  addWriteRequest(ring, con);
               } else {
                  addReadRequest(ring, con);
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

   int listenfd;
   int uring_wq_fd = -1;
   std::vector<std::thread> threads;

   void addAcceptRequest(io_uring& ring, int server_socket, struct sockaddr_in* client_addr, socklen_t* client_addr_len)
   {
      struct Connection* con = new Connection();
      con->eventType = EventType::accept;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_accept(sqe, server_socket, (struct sockaddr*)client_addr, client_addr_len, 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }

   void addReadRequest(io_uring& ring, Connection* con)
   {
      con->eventType = EventType::read;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_read(sqe, con->fd, con->parser.getReadBuffer(), Parser::maxPacketSize, 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }

   void addWriteRequest(io_uring& ring, Connection* con)
   {
      con->eventType = EventType::write;
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_write(sqe, con->fd, con->oBuffer.getBuffer(), con->oBuffer.size(), 0);
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);
   }
};
}  // namespace Net