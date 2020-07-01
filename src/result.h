#pragma once

#include <vector>
#include <functional>

#define RCPP_NEW_DATE_DATETIME_VECTORS 1
#include <Rcpp.h>
#include <clickhouse/client.h>

namespace ch = clickhouse;

class Converter;

using NullCol = std::shared_ptr<ch::ColumnNullable>;
using EnumItem = std::pair<std::string /* name */, int16_t /* value */>;

class Result {
  public:
  struct ColBlock {
    std::vector<ch::ColumnRef> columns;
  };

  private:
  using TypeList = std::vector<ch::TypeRef>;

  size_t fetchedRows = 0, // number of rows fetched so far
         availRows = 0;   // number of rows received from DB
  std::string statement;  // SQL statement corresponding to this result

  Rcpp::StringVector colNames;
  TypeList colTypes;
  Rcpp::StringVector colTypesString;
  std::vector<ColBlock> columnBlocks;

  void setColInfo(const ch::Block &block);

  using TypeAccFunc = std::function<ch::TypeRef(const TypeList &)>;

  public:
  using AccFunc = std::function<ch::ColumnRef(const ColBlock &)>;

  template<typename CT, typename RT>
  using ConvertFunc = std::function<void(const ColBlock &,
      std::shared_ptr<const CT>, RT &, size_t, size_t, size_t)>;

  Result(std::string stmt);

  template<typename CT, typename RT>
  void convertTypedColumn(AccFunc colAcc, Rcpp::List &df,
      size_t start, size_t len, ConvertFunc<CT, RT> convFunc) const;

  bool isComplete() const;
  size_t numFetchedRows() const;
  size_t numRowsAffected() const;
  std::string getStatement() const;
  TypeList getColTypes() const;

  void addBlock(const ch::Block &block);

  // build a converter tree for the given Clickhouse column type
  std::unique_ptr<Converter> buildConverter(std::string name, ch::TypeRef type) const;

  // build a data frame containing n entries from the result set, starting at
  // fetchedRows
  Rcpp::DataFrame fetchFrame(ssize_t n = -1);
};

// a nested converter structure used to convert a column to an R vector and add
// it to a data frame
class Converter {
public:
  // convert len column entries, beginning at start, from the blocks in r, and
  // add the resulting column to target
  virtual void processBlocks(Result &r, Result::AccFunc colAcc, Rcpp::List &target,
      size_t start, size_t len, Result::AccFunc nullAcc) = 0;

  // convert the given column to an R vector, which is inserted in the target
  // list at targetIdx
  virtual void processCol(ch::ColumnRef col, Rcpp::List &target, size_t targetIdx,
      NullCol nullCol) = 0;

  // avoid non-virtual destructor for this abstract class
  virtual ~Converter() {};
};
