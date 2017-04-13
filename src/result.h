#include <cassert>
#include <vector>
#include <stdexcept>
#include <Rcpp.h>
#include <clickhouse/client.h>
#include <ostream>

class Result {
  size_t fetchedRows = 0, // number of rows fetched so far
         availRows = 0;   // number of rows received from DB

  struct ColBlock {
    std::vector<clickhouse::ColumnRef> columns;
  };

  Rcpp::StringVector colNames;
  std::vector<ColBlock> columnBlocks;

  template<typename CT, int RT>
  void convertTypedColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len) {
    Rcpp::Vector<RT> v(len);   // R vector for the column

    size_t i = 0;
    for(ColBlock &cb : columnBlocks) {
      clickhouse::ColumnRef col = cb.columns[colIdx];

      if(i+col->Size() >= start) {  // only if this block was not fetched yet
        auto ccol = col->As<CT>();

        // first index within this block (note: can't use std::max(start-i, 0)
        // here, since the values are unsigned, and i>start is possible)
        size_t localStart = (i < start ? start-i : 0),
        // one past the last index within this block
        // guaranteed to be >=0, since the loop is aborted if i >= start+len
               localEnd = std::min(start+len-i, col->Size());

        for(size_t j = localStart; j < localEnd; j++) {
          v[i+j-start] = ccol->At(j);
        }
      }

      i += col->Size();
      if(i >= start+len) {  // processed all blocks in the requested range
        break;
      }
    }
    df.push_back(v);
  }

  // convert the given range of values from column colIdx to an R vector and
  // add it to the data frame df
  void convertColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len) {
    if(columnBlocks.empty()) {
      return;
    }

    using TC = clickhouse::Type::Code;
    clickhouse::TypeRef type = columnBlocks[0].columns[colIdx]->Type();
    switch(type->GetCode()) {
      case TC::Int8:
        convertTypedColumn<clickhouse::ColumnInt8, INTSXP>(colIdx, df, start, len);
        break;
      case TC::Int16:
        convertTypedColumn<clickhouse::ColumnInt16, INTSXP>(colIdx, df, start, len);
        break;
      case TC::Int32:
        convertTypedColumn<clickhouse::ColumnInt32, INTSXP>(colIdx, df, start, len);
        break;
      case TC::Int64:
        convertTypedColumn<clickhouse::ColumnInt64, INTSXP>(colIdx, df, start, len);
        break;
      case TC::UInt8:
        convertTypedColumn<clickhouse::ColumnUInt8, INTSXP>(colIdx, df, start, len);
        break;
      case TC::UInt16:
        convertTypedColumn<clickhouse::ColumnUInt16, INTSXP>(colIdx, df, start, len);
        break;
      case TC::UInt32:
        convertTypedColumn<clickhouse::ColumnUInt32, INTSXP>(colIdx, df, start, len);
        break;
      case TC::UInt64:
        convertTypedColumn<clickhouse::ColumnUInt64, INTSXP>(colIdx, df, start, len);
        break;
      case TC::String:
        convertTypedColumn<clickhouse::ColumnString, STRSXP>(colIdx, df, start, len);
        break;
      default:
        throw std::invalid_argument("unsupported type: "+type->GetName());
        break;
    }
    //TODO: release blocks once they have been fetched
  }

  void setColNames(const clickhouse::Block &block) {
    for(clickhouse::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
      colNames.push_back(Rcpp::String(bi.Name()));
    }
  }

  public:
  bool isComplete() { return fetchedRows >= availRows; }

  void addBlock(const clickhouse::Block &block) {
    if(colNames.size() < block.GetColumnCount()) {
      setColNames(block);
    }

    if(block.GetRowCount() > 0) {   // don't add empty blocks
      ColBlock cb;
      for(clickhouse::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        cb.columns.push_back(bi.Column());
      }
      columnBlocks.push_back(cb);
      availRows += block.GetRowCount();
    }
  }

  Rcpp::DataFrame fetchFrame(ssize_t n = -1) {
    size_t nRows = n > 0 ? std::min(static_cast<size_t>(n), availRows-fetchedRows) : availRows-fetchedRows;
    Rcpp::DataFrame df;

    for(size_t i = 0; i < colNames.size(); i++) {
      convertColumn(i, df, fetchedRows, nRows);
    }

    df.attr("class") = "data.frame";
    if(nRows > 0) {
      df.attr("row.names") = Rcpp::Range(fetchedRows+1, fetchedRows+nRows);
    }
    df.attr("names") = colNames;
    fetchedRows += nRows;

    return df;
  }
};
