//
// Created by swx on 24-1-5.
//

#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <boost/archive/text_iarchive.hpp>
#include <boost/archive/text_oarchive.hpp>
#include <boost/serialization/access.hpp>
#include <boost/serialization/vector.hpp>

#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#define STORE_FILE "store/dumpFile"

static std::string delimiter = ":";

template <typename K, typename V>
class Node {
 public:
  Node() : forward(nullptr), node_level(0) {}
  Node(K k, V v, int level);
  ~Node();

  K get_key() const;
  V get_value() const;
  void set_value(V value);

  Node<K, V>** forward;
  int node_level;

 private:
  K key;
  V value;
};

template <typename K, typename V>
Node<K, V>::Node(const K k, const V v, int level) {
  key = k;
  value = v;
  node_level = level;
  forward = new Node<K, V>*[level + 1];
  std::memset(forward, 0, sizeof(Node<K, V>*) * (level + 1));
}

template <typename K, typename V>
Node<K, V>::~Node() {
  delete[] forward;
}

template <typename K, typename V>
K Node<K, V>::get_key() const {
  return key;
}

template <typename K, typename V>
V Node<K, V>::get_value() const {
  return value;
}

template <typename K, typename V>
void Node<K, V>::set_value(V value) {
  this->value = value;
}

template <typename K, typename V>
class SkipListDump {
 public:
  void insert(const Node<K, V>& node);

  std::vector<K> keyDumpVt_;
  std::vector<V> valDumpVt_;

 private:
  friend class boost::serialization::access;

  template <class Archive>
  void serialize(Archive& ar, const unsigned int version) {
    ar& keyDumpVt_;
    ar& valDumpVt_;
  }
};

template <typename K, typename V>
class SkipList {
 public:
  explicit SkipList(int maxLevel);
  ~SkipList();

  int get_random_level();
  Node<K, V>* create_node(K key, V value, int level);
  int insert_element(K key, V value);
  void display_list();
  bool search_element(K key, V& value);
  void delete_element(K key);
  void insert_set_element(K& key, V& value);
  std::string dump_file();
  void load_file(const std::string& dumpStr);
  void clear(Node<K, V>* cur);
  int size();

 private:
  int insert_element_locked(const K& key, const V& value);
  bool delete_element_locked(const K& key);
  void get_key_value_from_string(const std::string& str, std::string* key, std::string* value);
  bool is_valid_string(const std::string& str);

  int _max_level;
  int _skip_list_level;
  Node<K, V>* _header;
  std::ofstream _file_writer;
  std::ifstream _file_reader;
  int _element_count;
  mutable std::mutex _mtx;
};

template <typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K key, const V value, int level) {
  return new Node<K, V>(key, value, level);
}

template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
  std::lock_guard<std::mutex> lock(_mtx);
  return insert_element_locked(key, value);
}

template <typename K, typename V>
int SkipList<K, V>::insert_element_locked(const K& key, const V& value) {
  Node<K, V>* current = _header;

  std::vector<Node<K, V>*> update(_max_level + 1, nullptr);
  for (int i = _skip_list_level; i >= 0; --i) {
    while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current != nullptr && current->get_key() == key) {
    return 1;
  }

  int randomLevel = get_random_level();
  if (randomLevel > _skip_list_level) {
    for (int i = _skip_list_level + 1; i <= randomLevel; ++i) {
      update[i] = _header;
    }
    _skip_list_level = randomLevel;
  }

  Node<K, V>* insertedNode = create_node(key, value, randomLevel);
  for (int i = 0; i <= randomLevel; ++i) {
    insertedNode->forward[i] = update[i]->forward[i];
    update[i]->forward[i] = insertedNode;
  }
  ++_element_count;
  return 0;
}

template <typename K, typename V>
void SkipList<K, V>::display_list() {
  std::lock_guard<std::mutex> lock(_mtx);
  std::cout << "\n*****Skip List*****\n";
  for (int i = 0; i <= _skip_list_level; ++i) {
    Node<K, V>* node = _header->forward[i];
    std::cout << "Level " << i << ": ";
    while (node != nullptr) {
      std::cout << node->get_key() << ":" << node->get_value() << ";";
      node = node->forward[i];
    }
    std::cout << std::endl;
  }
}

template <typename K, typename V>
std::string SkipList<K, V>::dump_file() {
  std::lock_guard<std::mutex> lock(_mtx);
  Node<K, V>* node = _header->forward[0];
  SkipListDump<K, V> dumper;
  while (node != nullptr) {
    dumper.insert(*node);
    node = node->forward[0];
  }

  std::stringstream ss;
  boost::archive::text_oarchive oa(ss);
  oa << dumper;
  return ss.str();
}

template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string& dumpStr) {
  SkipListDump<K, V> dumper;
  if (!dumpStr.empty()) {
    std::stringstream iss(dumpStr);
    boost::archive::text_iarchive ia(iss);
    ia >> dumper;
  }

  std::lock_guard<std::mutex> lock(_mtx);
  if (_header->forward[0] != nullptr) {
    clear(_header->forward[0]);
  }
  for (int i = 0; i <= _max_level; ++i) {
    _header->forward[i] = nullptr;
  }
  _skip_list_level = 0;
  _element_count = 0;

  for (std::size_t i = 0; i < dumper.keyDumpVt_.size(); ++i) {
    insert_element_locked(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
  }
}

template <typename K, typename V>
int SkipList<K, V>::size() {
  std::lock_guard<std::mutex> lock(_mtx);
  return _element_count;
}

template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key, std::string* value) {
  if (!is_valid_string(str)) {
    return;
  }
  *key = str.substr(0, str.find(delimiter));
  *value = str.substr(str.find(delimiter) + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str) {
  return !str.empty() && str.find(delimiter) != std::string::npos;
}

template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
  std::lock_guard<std::mutex> lock(_mtx);
  delete_element_locked(key);
}

template <typename K, typename V>
bool SkipList<K, V>::delete_element_locked(const K& key) {
  Node<K, V>* current = _header;
  std::vector<Node<K, V>*> update(_max_level + 1, nullptr);

  for (int i = _skip_list_level; i >= 0; --i) {
    while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
    update[i] = current;
  }

  current = current->forward[0];
  if (current == nullptr || current->get_key() != key) {
    return false;
  }

  for (int i = 0; i <= _skip_list_level; ++i) {
    if (update[i]->forward[i] != current) {
      break;
    }
    update[i]->forward[i] = current->forward[i];
  }

  while (_skip_list_level > 0 && _header->forward[_skip_list_level] == nullptr) {
    --_skip_list_level;
  }

  delete current;
  --_element_count;
  return true;
}

template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K& key, V& value) {
  std::lock_guard<std::mutex> lock(_mtx);
  delete_element_locked(key);
  insert_element_locked(key, value);
}

template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V& value) {
  std::lock_guard<std::mutex> lock(_mtx);
  Node<K, V>* current = _header;

  for (int i = _skip_list_level; i >= 0; --i) {
    while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
      current = current->forward[i];
    }
  }

  current = current->forward[0];
  if (current != nullptr && current->get_key() == key) {
    value = current->get_value();
    return true;
  }
  return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V>& node) {
  keyDumpVt_.emplace_back(node.get_key());
  valDumpVt_.emplace_back(node.get_value());
}

template <typename K, typename V>
SkipList<K, V>::SkipList(int maxLevel) {
  _max_level = maxLevel;
  _skip_list_level = 0;
  _element_count = 0;

  K key;
  V value;
  _header = new Node<K, V>(key, value, _max_level);
}

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
  if (_file_writer.is_open()) {
    _file_writer.close();
  }
  if (_file_reader.is_open()) {
    _file_reader.close();
  }

  if (_header->forward[0] != nullptr) {
    clear(_header->forward[0]);
  }
  delete _header;
}

template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V>* cur) {
  if (cur->forward[0] != nullptr) {
    clear(cur->forward[0]);
  }
  delete cur;
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
  int level = 1;
  while (std::rand() % 2) {
    ++level;
  }
  return level < _max_level ? level : _max_level;
}

#endif  // SKIPLIST_H
