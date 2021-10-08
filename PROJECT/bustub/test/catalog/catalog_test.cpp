//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// catalog_test.cpp
//
// Identification: test/catalog/catalog_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>
#include <unordered_set>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "gtest/gtest.h"
#include "type/value_factory.h"

namespace bustub {

// NOLINTNEXTLINE
TEST(CatalogTest, CreateTableTest) {
  auto disk_manager = new DiskManager("catalog_test.db");
  auto bpm = new BufferPoolManager(32, disk_manager);
  auto catalog = new Catalog(bpm, nullptr, nullptr);
  std::string table_name = "potato";
  std::string index_name = "tomato";

  // The table shouldn't exist in the catalog yet.
  EXPECT_THROW(catalog->GetTable(table_name), std::out_of_range);

  // Put the table into the catalog.
  std::vector<Column> columns;
  std::vector<uint32_t> key_attrs;
  columns.emplace_back("A", TypeId::INTEGER);
  columns.emplace_back("B", TypeId::BOOLEAN);
  key_attrs.emplace_back(0);

  Schema schema(columns);
  auto *table_metadata = catalog->CreateTable(nullptr, table_name, schema);
  auto *get_table_metadata = catalog->GetTable(table_name);

  EXPECT_EQ(get_table_metadata, table_metadata);
  // Notice that this test case doesn't check anything! :(
  // It is up to you to extend it

  auto *indexinfo = catalog->CreateIndex<GenericKey<8>, RID, GenericComparator<8>>(nullptr, index_name, table_name,
                                                                                   schema, schema, key_attrs, 8);
  auto *get_indexinfo = catalog->GetIndex(index_name, table_name);
  EXPECT_EQ(indexinfo, get_indexinfo);

  delete catalog;
  delete bpm;
  delete disk_manager;
}

}  // namespace bustub
