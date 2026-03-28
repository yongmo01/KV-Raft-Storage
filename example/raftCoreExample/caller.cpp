//
// Created by swx on 23-6-4.
// Updated for dynamic CLI arguments.
//
#include <iostream>
#include <string>
#include <unistd.h>
#include <chrono>
#include "clerk.h"
#include "util.h"
#include "config.h"

void ShowArgsHelp() {
  std::cout << "Usage: ./callerMain [options]\n"
            << "Options:\n"
            << "  -c <count>      Number of iterations (default: 500)\n"
            << "  -o <operation>  Operation mode: 'both' (Put & Get), 'put' (Only Put), 'get' (Only Get), 'ttl' (PutWithTTL). (default: both)\n"
            << "  -k <key>        Key to operate on (default: 'x')\n"
            << "  -t <ttl_ms>     TTL value in milliseconds for 'ttl' mode (default: 3000)\n"
            << "  -f <conf_file>  Configuration file (default: 'test.conf')\n"
            << "  -h              Show this help message\n";
}

int main(int argc, char **argv) {
  int count = 500;
  std::string op_mode = "both";
  std::string key = "x";
  int ttl_ms = 3000;
  std::string conf_file = "test.conf";

  int opt;
  while ((opt = getopt(argc, argv, "c:o:k:t:f:h")) != -1) {
    switch (opt) {
      case 'c': count = std::stoi(optarg); break;
      case 'o': op_mode = optarg; break;
      case 'k': key = optarg; break;
      case 't': ttl_ms = std::stoi(optarg); break;
      case 'f': conf_file = optarg; break;
      case 'h':
      default:
        ShowArgsHelp();
        return 0;
    }
  }

  Clerk client;
  client.Init(conf_file);
  auto start_time = std::chrono::high_resolution_clock::now();

  std::cout << "=====================================\n";
  std::cout << "Starting Client Test\n";
  std::cout << "Count: " << count << " | Mode: " << op_mode << " | Key: " << key << " | Conf: " << conf_file << "\n";
  std::cout << "=====================================\n";

  for (int i = count; i > 0; --i) {
    std::string val = std::to_string(i);
    
    if (op_mode == "both" || op_mode == "put") {
      client.Put(key, val);
    } else if (op_mode == "ttl") {
#if ENABLE_KEY_TTL
      client.PutWithTTL(key, val, ttl_ms);
#else
      client.Put(key, val); 
#endif
    }
    
    if (op_mode == "both" || op_mode == "get" || op_mode == "ttl") {
      std::string get_val = client.Get(key);
      if (count <= 100) { 
        std::printf("Get return : {%s}\r\n", get_val.c_str());
      }
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
  std::cout << "Finished " << count << " operations in " << elapsed_ms << " ms." << std::endl;

  return 0;
}