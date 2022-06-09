#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
public:
  // you may define your own constructor based on your member variables
  TableIterator() = delete;

  TableIterator(TableHeap *table_heap, Row row);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator == (const TableIterator &itr) const;

  bool operator != (const TableIterator &itr) const;

  void operator = (const TableIterator &itr) { 
    table_heap_ = itr.table_heap_;
    row_ = itr.row_;
  }

  const Row &operator*();

  Row *operator->();

  /**
   * @brief 移动到下一条记录，通过++iter调用
   * @return TableIterator& 
   */
  TableIterator &operator++();

  /**
   * @brief 移动到下一条记录，通过iter++调用
   * @return TableIterator 
   */
  TableIterator operator++(int);

private:
  TableHeap *table_heap_;
  Row *row_;
};

#endif //MINISQL_TABLE_ITERATOR_H
