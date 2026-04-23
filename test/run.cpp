//
// Created by swx on 24-1-5.
//

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>

#include <fstream>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

class SerializablePair {
 public:
  SerializablePair() = default;
  SerializablePair(const std::string& first, const std::string& second) : first(first), second(second) {}

  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& first;
    ar& second;
  }

 private:
  std::string first;
  std::string second;
};

int main() {
  std::vector<std::pair<std::string, std::string>> data;
  data.emplace_back("key1", "value1");
  data.emplace_back("key2", "value2");
  data.emplace_back("key3", "value3");

  std::ofstream ofs("data_vector.txt");
  boost::archive::text_oarchive oa(ofs);
  oa << data;
  ofs.close();

  std::ifstream ifs("data_vector.txt");
  boost::archive::text_iarchive ia(ifs);

  std::vector<std::pair<std::string, std::string>> loadedData;
  ia >> loadedData;

  for (const auto& pair : loadedData) {
    std::cout << "Key: " << pair.first << ", Value: " << pair.second << std::endl;
  }

  return 0;
}
