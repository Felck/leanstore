#pragma once
#include <arpa/inet.h>
#include <fcntl.h>
#include <liburing.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
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

      // register listening socket
      struct epoll_event ev;
      memset(&ev, 0, sizeof(ev));
      ev.data.fd = listenfd;
      ev.events = EPOLLIN | EPOLLEXCLUSIVE;

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
      volatile uint64_t readCnt = 0;
      volatile uint64_t sendCnt = 0;

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
            fprintf(stderr, "Async request failed: %s for event: %d\n", strerror(-cqe->res), req->event_type);
            exit(EXIT_FAILURE);
         }

         switch (req->event_type) {
            case EventType::accept:
               con->fd = cqe->res;
               addReadRequest(con);
               addAcceptRequest(listenfd, &clientaddr, &clilen);
               break;
            case EventType::read:
               if (!cqe->res) {
                  fprintf(stderr, "Empty request!\n");
                  break;
               }
               handle_client_request(con);
               // TODO delete
               delete req->iov[0].iov_base;
               delete req;
               break;
            case EventType::write:
               for (int i = 0; i < req->iovec_count; i++) {
                  free(req->iov[i].iov_base);
               }
               close(req->fd);
               delete req;
               break;
         }
         // mark this request as processed
         io_uring_cqe_seen(&ring, cqe);
      }
   }

  private:
   enum class EventType : int { accept = 0, read = 1, write = 2 };

   struct Connection {
      ssize_t send() { return oBuffer.send(fd); };

      OBuffer<uint8_t> oBuffer;
      Parser parser{oBuffer};
      EventType eventType = EventType::accept;
      int fd;
   };

   struct io_uring ring;
   int listenfd;
   std::vector<std::thread> threads;
   std::unordered_map<int, Connection> connections;
   pthread_rwlock_t connLatch;

   void closeConnection(int fd)
   {
      pthread_rwlock_wrlock(&connLatch);
      connections.erase(fd);
      pthread_rwlock_unlock(&connLatch);
      close(fd);
   }

   int addAcceptRequest(int server_socket, struct sockaddr_in* client_addr, socklen_t* client_addr_len)
   {
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      io_uring_prep_accept(sqe, server_socket, (struct sockaddr*)client_addr, client_addr_len, 0);
      struct Connection* con = new Connection();
      io_uring_sqe_set_data(sqe, con);
      io_uring_submit(&ring);

      return 0;
   }

   int addReadRequest(Connection* con)
   {
      struct io_uring_sqe* sqe = io_uring_get_sqe(&ring);
      con->eventType = EventType::read;
      memset(req->iov[0].iov_base, 0, READ_SZ);
      /* Linux kernel 5.5 has support for readv, but not for recv() or read() */
      io_uring_prep_readv(sqe, client_socket, &req->iov[0], 1, 0);
      io_uring_sqe_set_data(sqe, req);
      io_uring_submit(&ring);
      return 0;
   }
};
}  // namespace Net