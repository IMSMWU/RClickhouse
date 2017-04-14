#include <vector>
#include <functional>

#include <Rcpp.h>
#include <clickhouse/client.h>

class Result {
  size_t fetchedRows = 0, // number of rows fetched so far
         availRows = 0;   // number of rows received from DB

  struct ColBlock {
    std::vector<clickhouse::ColumnRef> columns;
  };

  Rcpp::StringVector colNames;
  std::vector<ColBlock> columnBlocks;

  // convert the given range of values from column colIdx to an R vector and
  // add it to the data frame df
  void convertColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len);

  void setColNames(const clickhouse::Block &block);

  public:
  template<typename CT, typename RT>
  void convertTypedColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len, std::function<void(std::shared_ptr<CT>, RT &, size_t, size_t, size_t)> convFunc);

  bool isComplete();

  void addBlock(const clickhouse::Block &block);
  Rcpp::DataFrame fetchFrame(ssize_t n = -1);
};
