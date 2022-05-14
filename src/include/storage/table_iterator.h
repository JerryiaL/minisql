#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  inline bool operator==(const TableIterator &itr) const;

  inline bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  /**
   * @brief 移动到下一条记录，通过++iter调用
   * 
   * @return TableIterator& 
   */
  TableIterator &operator++();

  /**
   * @brief 移动到下一条记录，通过iter++调用
   * 
   * @return TableIterator 
   */
  TableIterator operator++(int);

private:
  // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H
