#include <unistd.h>

#include <iostream>
#include <set>
#include <string>
#include <vector>

#include "Parser.hpp"
#include "PerfEvent.hpp"
#include "leanstore/concurrency-recovery/CRMG.hpp"
#include "leanstore/profiling/counters/CPUCounters.hpp"
#include "leanstore/profiling/counters/WorkerCounters.hpp"
#include "leanstore/utils/Misc.hpp"
#include "leanstore/utils/Parallelize.hpp"
#include "leanstore/utils/RandomGenerator.hpp"
#include "leanstore/utils/ZipfGenerator.hpp"
#include "server.hpp"
#include "workload.hpp"
// -------------------------------------------------------------------------------------
DEFINE_uint32(port, 8888, "");
DEFINE_uint64(run_until_tx, 0, "");
DEFINE_bool(tpcc_warehouse_affinity, false, "");
DEFINE_bool(tpcc_fast_load, false, "");
// -------------------------------------------------------------------------------------
using namespace std;
using namespace leanstore;
using namespace TPCC;
// -------------------------------------------------------------------------------------
double calculateMTPS(chrono::high_resolution_clock::time_point begin, chrono::high_resolution_clock::time_point end, u64 factor)
{
   double tps = ((factor * 1.0 / (chrono::duration_cast<chrono::microseconds>(end - begin).count() / 1000000.0)));
   return (tps / 1000000.0);
}
// -------------------------------------------------------------------------------------
int main(int argc, char** argv)
{
   gflags::SetUsageMessage("Leanstore TPC-C");
   gflags::ParseCommandLineFlags(&argc, &argv, true);
   // -------------------------------------------------------------------------------------
   LeanStore db;
   auto& crm = db.getCRManager();
   // -------------------------------------------------------------------------------------
   crm.scheduleJobSync(0, [&]() {
      warehouse = LeanStoreAdapter<warehouse_t>(db, "warehouse");
      district = LeanStoreAdapter<district_t>(db, "district");
      customer = LeanStoreAdapter<customer_t>(db, "customer");
      customerwdl = LeanStoreAdapter<customer_wdl_t>(db, "customerwdl");
      history = LeanStoreAdapter<history_t>(db, "history");
      neworder = LeanStoreAdapter<neworder_t>(db, "neworder");
      order = LeanStoreAdapter<order_t>(db, "order");
      order_wdc = LeanStoreAdapter<order_wdc_t>(db, "order_wdc");
      orderline = LeanStoreAdapter<orderline_t>(db, "orderline");
      item = LeanStoreAdapter<item_t>(db, "item");
      stock = LeanStoreAdapter<stock_t>(db, "stock");
   });
   // -------------------------------------------------------------------------------------
   db.registerConfigEntry("tpcc_warehouse_count", FLAGS_tpcc_warehouse_count);
   db.registerConfigEntry("tpcc_warehouse_affinity", FLAGS_tpcc_warehouse_affinity);
   db.registerConfigEntry("run_until_tx", FLAGS_run_until_tx);
   // -------------------------------------------------------------------------------------
   // const u64 load_threads = (FLAGS_tpcc_fast_load) ? thread::hardware_concurrency() : FLAGS_worker_threads;
   {
      crm.scheduleJobSync(0, [&]() {
         cr::Worker::my().startTX();
         loadItem();
         loadWarehouse();
         cr::Worker::my().commitTX();
      });
      std::atomic<u32> g_w_id = 1;
      for (u32 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         crm.scheduleJobAsync(t_i, [&]() {
            while (true) {
               u32 w_id = g_w_id++;
               if (w_id > FLAGS_tpcc_warehouse_count) {
                  return;
               }
               cr::Worker::my().startTX();
               loadStock(w_id);
               loadDistrinct(w_id);
               for (Integer d_id = 1; d_id <= 10; d_id++) {
                  loadCustomer(w_id, d_id);
                  loadOrders(w_id, d_id);
               }
               cr::Worker::my().commitTX();
            }
         });
      }
      crm.joinAll();
   }
   sleep(2);
   // -------------------------------------------------------------------------------------
   double gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << "data loaded - consumed space in GiB = " << gib << endl;
   crm.scheduleJobSync(0, [&]() { cout << "Warehouse pages = " << warehouse.btree->countPages() << endl; });
   // -------------------------------------------------------------------------------------
   Net::Server<Parser> server;
   server.init(FLAGS_port);

   atomic<bool> keep_running = true;
   atomic<u64> running_threads_counter = 0;
   vector<thread> threads;
   auto random = std::make_unique<leanstore::utils::ZipfGenerator>(FLAGS_tpcc_warehouse_count, FLAGS_zipf_factor);
   db.startProfilingThread();
   u64 tx_per_thread[FLAGS_worker_threads];

   for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
      crm.scheduleJobAsync(t_i, [&, t_i]() {
         running_threads_counter++;
         volatile u64 tx_acc = 0;
         cr::Worker::my().refreshSnapshot();
         server.runThread(keep_running, [&]() {
            WorkerCounters::myCounters().tx++;
            tx_acc++;
         });
         tx_per_thread[t_i] = tx_acc;
         running_threads_counter--;
      });
   }
   {
      if (FLAGS_run_until_tx) {
         while (true) {
            if (db.getGlobalStats().accumulated_tx_counter >= FLAGS_run_until_tx) {
               cout << FLAGS_run_until_tx << " has been reached";
               break;
            }
            usleep(500);
         }
      } else {
         // Shutdown threads
         sleep(FLAGS_run_for_seconds);
      }
      keep_running = false;
      while (running_threads_counter) {
         MYPAUSE();
      }
      crm.joinAll();
   }
   {
      cout << endl;
      for (u64 t_i = 0; t_i < FLAGS_worker_threads; t_i++) {
         cout << tx_per_thread[t_i] << ",";
      }
      cout << endl;
   }
   // -------------------------------------------------------------------------------------
   gib = (db.getBufferManager().consumedPages() * EFFECTIVE_PAGE_SIZE / 1024.0 / 1024.0 / 1024.0);
   cout << endl << "consumed space in GiB = " << gib << endl;
   // -------------------------------------------------------------------------------------
   if (FLAGS_persist) {
      db.getBufferManager().writeAllBufferFrames();
   }
   return 0;
}
