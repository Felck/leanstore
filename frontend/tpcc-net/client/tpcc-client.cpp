#include <arpa/inet.h>
#include <gflags/gflags.h>
#include <pthread.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>

#include "Parser.hpp"
#include "workload.hpp"

DEFINE_string(ip, "localhost", "Server IP adress");
DEFINE_uint32(port, 8888, "Server port");
DEFINE_uint32(threads, 4, "Number of threads");
DEFINE_uint32(run_for_secs, 0, "");
DEFINE_uint32(tpcc_warehouse_count, 1, "");

void writeMessage(int fd, std::vector<uint8_t>& msg)
{
   size_t written = 0;
   ssize_t n;
   while (written != msg.size()) {
      if ((n = write(fd, &msg[0] + written, msg.size() - written)) < 0) {
         perror("write()");
         exit(EXIT_FAILURE);
      } else {
         written += n;
      }
   }
}

void runThread(int t_i, std::atomic<bool>& keepRunning)
{
   // init socket
   struct sockaddr_in address;
   memset(&address, 0, sizeof(address));
   address.sin_family = AF_INET;
   inet_aton(FLAGS_ip.c_str(), &(address.sin_addr));
   address.sin_port = htons(FLAGS_port);
   // connect to server
   int sockfd = socket(AF_INET, SOCK_STREAM, 0);
   if (connect(sockfd, (struct sockaddr*)&address, sizeof(address)) == -1) {
      perror("connect()");
      exit(EXIT_FAILURE);
   }

   // main loop
   char iBuf[TPCC::Parser::maxPacketSize] = {0};
   std::vector<uint8_t> oBuf;
   TPCC::Parser parser;

   TPCC::TransactionType t = TPCC::tx(oBuf, TPCC::urand(1, FLAGS_tpcc_warehouse_count));
   writeMessage(sockfd, oBuf);
   oBuf.clear();

   while (keepRunning) {
      ssize_t n = read(sockfd, iBuf, TPCC::Parser::maxPacketSize);
      if (n == -1) {
         // error
         perror("read: ");
         keepRunning = false;
      } else if (n == 0) {
         // con closed
         std::cout << "con closed" << std::endl;
         keepRunning = false;
      } else {
         parser.parse(reinterpret_cast<uint8_t*>(iBuf), n, [&]() {
            if (parser.getTxType() == TPCC::TransactionType::error) {
               // std::cout << "Error with TxType " << static_cast<int>(t) << std::endl;
            } else {
               assert(t == parser.getTxType());
            }
            t = TPCC::tx(oBuf, TPCC::urand(1, FLAGS_tpcc_warehouse_count));
            writeMessage(sockfd, oBuf);
            oBuf.clear();
         });
      }
   }

   close(sockfd);
}

int main(int argc, char* argv[])
{
   gflags::SetUsageMessage("Leanstore TPC-C Client");
   gflags::ParseCommandLineFlags(&argc, &argv, true);

   // start threads
   std::vector<std::thread> threads;
   std::atomic<bool> keepRunning{true};
   for (uint t_i = 0; t_i < FLAGS_threads; t_i++) {
      threads.emplace_back(runThread, t_i, std::ref(keepRunning));
   }

   if (FLAGS_run_for_secs) {
      sleep(FLAGS_run_for_secs);
      keepRunning = false;
   }

   for (auto& thread : threads) {
      thread.join();
   }

   return 0;
}
