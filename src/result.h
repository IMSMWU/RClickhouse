#pragma once

#include <vector>
#include <functional>

#define RCPP_NEW_DATE_DATETIME_VECTORS 1
#include <Rcpp.h>
#include <clickhouse/client.h>

namespace ch = clickhouse;

class Result {
  public:

  struct ColBlock {
    std::vector<ch::ColumnRef> columns;
  };

  private:
  using TypeList = std::vector<ch::TypeRef>;

  size_t fetchedRows = 0, // number of rows fetched so far
         availRows = 0;   // number of rows received from DB

  Rcpp::StringVector colNames;
  TypeList colTypes;
  std::vector<ColBlock> columnBlocks;

  void setColInfo(const ch::Block &block);

  using TypeAccFunc = std::function<ch::TypeRef(const TypeList &)>;

  public:
  using AccFunc = std::function<ch::ColumnRef(const ColBlock &)>;

  template<typename CT, typename RT>
  using ConvertFunc = std::function<void(const ColBlock &,
      std::shared_ptr<const CT>, RT &, size_t, size_t, size_t)>;

  template<typename CT, typename RT>
  void convertTypedColumn(AccFunc colAcc, Rcpp::DataFrame &df,
      size_t start, size_t len, ConvertFunc<CT, RT> convFunc) const;

  bool isComplete() const;
  size_t numFetchedRows() const;

  void addBlock(const ch::Block &block);

  // convert the given range of values from the column provided by colAcc to an
  // R vector and add it to the data frame df
  void convertColumn(AccFunc colAcc, TypeAccFunc typeAcc, Rcpp::DataFrame &df,
      size_t start, size_t len, AccFunc nullAcc = nullptr) const;

  // build a data frame containing n entries from the result set, starting at
  // fetchedRows
  Rcpp::DataFrame fetchFrame(ssize_t n = -1);
};
