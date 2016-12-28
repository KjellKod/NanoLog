#include <chrono>
#include <thread>
#include <vector>
#include <cstdio>
#include <algorithm>
#include <cstring>
#include "NanoLog.hpp"
#include <g3log/g3log.hpp>
#include <g3log/logworker.hpp>
#include <Vampire.h>
#include <Rifle.h>
#include <vector>
#include <memory>
#include <atomic>
#include <future>
#include <sstream>
#include <iostream>
#include <chrono>


/* Returns microseconds since epoch */
uint64_t timestamp_now() {
   return std::chrono::high_resolution_clock::now().time_since_epoch() / std::chrono::microseconds(1);
}

template < typename Function >
void run_log_benchmark(Function&& f, char const* const logger) {
   int iterations = 100000;
   std::vector < uint64_t > latencies;
   char const* const benchmark = "benchmark";
   for (int i = 0; i < iterations; ++i) {
      uint64_t begin = timestamp_now();
      f(i, benchmark);
      uint64_t end = timestamp_now();
      latencies.push_back(end - begin);
   }
   std::sort(latencies.begin(), latencies.end());
   uint64_t sum = 0; for (auto l : latencies) { sum += l; }
   printf("%s percentile latency numbers in microseconds\n%9s|%9s|%9s|%9s|%9s|%9s|%9s|\n%9ld|%9ld|%9ld|%9ld|%9ld|%9ld|%9lf|\n"
          , logger
          , "50th"
          , "75th"
          , "90th"
          , "99th"
          , "99.9th"
          , "Worst"
          , "Average"
          , latencies[(size_t)iterations * 0.5]
          , latencies[(size_t)iterations * 0.75]
          , latencies[(size_t)iterations * 0.9]
          , latencies[(size_t)iterations * 0.99]
          , latencies[(size_t)iterations * 0.999]
          , latencies[latencies.size() - 1]
          , (sum * 1.0) / latencies.size()
         );
}

template < typename Function >
void run_benchmark(Function&& f, int thread_count, char const* const logger) {
   printf("\nThread count: %d\n", thread_count);
   std::vector < std::thread > threads;
   for (int i = 0; i < thread_count; ++i) {
      threads.emplace_back(run_log_benchmark<Function>, std::ref(f), logger);
   }
   for (int i = 0; i < thread_count; ++i) {
      threads[i].join();
   }
}

void print_usage() {
   char const* const executable = "nano_vs_g3log";
   printf("Usage \n1. %s nanolog\n2. %s g3log\n3. %s g3logstream\n4. queuenado\n", executable, executable, executable, executable);
}



std::string gQueueNadoLocation = "ipc:///tmp/nanologtest.ipc";
Rifle* CreateSendQueue() {
   Rifle* serverQueue = new Rifle(gQueueNadoLocation.c_str());
   serverQueue->SetOwnSocket(false);
   int size = (100 * 500 * 5);
   serverQueue->SetHighWater(size);
   if (!serverQueue->Aim()) {
      delete serverQueue;
      serverQueue = nullptr;
      std::cerr << "ERROR : " << __FUNCTION__ << " L: " << __LINE__ << std::endl;
      return nullptr;
   }
   return serverQueue;
}




thread_local std::atomic<bool> gKeepRunning = {true};
std::atomic<bool> gWaitToStart = {true};

void Send(std::string message) {
   thread_local std::unique_ptr<Rifle> sender(CreateSendQueue());
   if (sender == nullptr) {
      std::cerr << "ERROR : " << __FUNCTION__ << " L: " << __LINE__ << std::endl;
      return;
   }

   void* data = nullptr;
   std::string* copymsg = new std::string(message);
   data = reinterpret_cast<void*>(copymsg);
   sender->FireStake(data, 0);
}


size_t Receive() {
   Vampire receiveQ (gQueueNadoLocation.c_str());
   receiveQ.SetOwnSocket(true);
   receiveQ.SetHighWater(100 * 500 * 5);
   size_t kMaxWait = 20;
   size_t waitCounter = 0;
   if (!receiveQ.PrepareToBeShot()) {
      std::cerr << "cannot start receiver on " << "ipc:///tmp/nanologtest.ipc" << std::endl;
      return 0;
   }

   size_t received = 0;
   gWaitToStart.store(false, std::memory_order_seq_cst);
   while (gKeepRunning.load() && waitCounter < 20) {
      void* data = nullptr;
      receiveQ.GetStake(data, 1000);
      if (data != nullptr) {
         ++received;
         std::string* str = reinterpret_cast<std::string*>(data);
         delete str;
      } else {
        ++waitCounter;
      }
   }
   return received;
}


int main(int argc, char* argv[]) {
   if (argc != 2) {
      print_usage();
      return 0;
   }

   if (strcmp(argv[1], "nanolog") == 0) {
      // Guaranteed nano log.
      nanolog::initialize(nanolog::GuaranteedLogger(), "/tmp/", "nanolog", 1);

      auto nanolog_benchmark = [](int i, char const * const cstr) {  LOG_INFO << "Logging " << cstr << i << 0 << 'K' << -42.42; };
      for (auto threads : { 1, 2, 3, 4 })
         run_benchmark(nanolog_benchmark, threads, "nanolog_guaranteed");
   } else if (strcmp(argv[1], "g3log") == 0) {
      auto worker = g3::LogWorker::createLogWorker();
      auto handle = worker->addDefaultLogger("g3", "/tmp/");
      g3::initializeLogging(worker.get());

      auto g3log_benchmark = [](int i, char const * const cstr) { LOGF(INFO, "Logging %s%d%d%c%lf", cstr, i, 0, 'K', -42.42); };
      for (auto threads : { 1, 2, 3, 4 })
         run_benchmark(g3log_benchmark, threads, "g3log");
   } else if (strcmp(argv[1], "g3logstream") == 0) {
      auto worker = g3::LogWorker::createLogWorker();
      auto handle = worker->addDefaultLogger("g3", "/tmp/");
      g3::initializeLogging(worker.get());

      auto g3log_benchmark2 = [](int i, char const * const cstr) {  LOG(INFO) << "Logging " << cstr << i << 0 << 'K' << -42.42; };
      for (auto threads : { 1, 2, 3, 4 })
         run_benchmark(g3log_benchmark2, threads, "g3log");


   } else if (strcmp(argv[1], "queuenado") == 0) {
      auto doneReceiving = std::async(std::launch::async, &Receive);
      size_t waited = 0;
      while(gWaitToStart.load(std::memory_order_seq_cst) && waited < 20) {
         ++waited;
         std::this_thread::sleep_for(std::chrono::seconds(1));
      }
      if (gWaitToStart.load(std::memory_order_seq_cst)) {
        std::cerr << "L: " << __LINE__ << " func: " << __FUNCTION__ << " failure to start the test (receiver side)" << std::endl; 
      } 
      auto queuenado = [](int i, char const * const cstr) {
         std::ostringstream oss;
         oss  << "Logging " << cstr << i << 0 << 'K' << -42.42;
         Send(oss.str());
      };
      {
         for (auto threads : { 1, 2, 3, 4 })
            run_benchmark(queuenado, threads, "queuenado");

      }                
      gKeepRunning.store(false);
      auto received = doneReceiving.get();
      std::cout << "Number of received entries: " << received << std::endl;

   } else {
      print_usage();
   }


   return 0;
}

