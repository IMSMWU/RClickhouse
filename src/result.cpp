#include <vector>
#include <stdexcept>
#include <functional>

#include <Rcpp.h>
#include <clickhouse/client.h>

#include "result.h"

// helper function for converting the column data (can't be a member of Result
// due to C++ prohibiting explicit specialization on members of a
// non-specialized class)
template<typename CT, typename RT>
inline void convertCol(Result &r, size_t colIdx, Rcpp::DataFrame &df,
    size_t start, size_t len) {
  r.convertTypedColumn<CT, RT>(colIdx, df, start, len,
    [](std::shared_ptr<CT> in, RT &out, size_t offset, size_t start, size_t end) {
      for(size_t j = start; j < end; j++) {
        out[offset+j-start] = in->At(j);
      }
    }
  );
}

// Date requires specialization: otherwise causes problems due to type
// ambiguities in the Rcpp::Date constructor, which expects either int or
// double, whereas ColumnDate values are uint32_t
template<>
inline void convertCol<clickhouse::ColumnDate, Rcpp::DateVector>(Result &r,
    size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len) {
  using CT = clickhouse::ColumnDate;
  using RT = Rcpp::DateVector;
  r.convertTypedColumn<CT, RT>(colIdx, df, start, len,
    [](std::shared_ptr<CT> in, RT &out, size_t offset, size_t start, size_t end) {
      for(size_t j = start; j < end; j++) {
        out[offset+j-start] = static_cast<int>(in->At(j)/(60*60*24));
      }
    }
  );
}

template<typename CT, typename RT>
void Result::convertTypedColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len, std::function<void(std::shared_ptr<CT>, RT &, size_t, size_t, size_t)> convFunc) {
  RT v(len);   // R vector for the column

  size_t i = 0;
  for(ColBlock &cb : columnBlocks) {
    clickhouse::ColumnRef col = cb.columns[colIdx];

    if(i+col->Size() >= start) {  // only if this block was not fetched yet
      auto ccol = col->As<CT>();

      // first index within this block (note: can't use std::max(start-i, 0)
      // here, since the variables are unsigned, and i>start is possible)
      size_t localStart = (i < start ? start-i : 0),
      // one past the last index within this block
      // guaranteed to be >=0, since the loop is aborted if i >= start+len
             localEnd = std::min(start+len-i, col->Size());

      convFunc(ccol, v, i, localStart, localEnd);
    }

    i += col->Size();
    if(i >= start+len) {  // processed all blocks in the requested range
      break;
    }
  }
  df.push_back(v);
}

void Result::convertColumn(size_t colIdx, Rcpp::DataFrame &df, size_t start, size_t len) {
  using TC = clickhouse::Type::Code;
  clickhouse::TypeRef type = colTypes[colIdx];
  switch(type->GetCode()) {
    case TC::Int8:
      convertCol<clickhouse::ColumnInt8, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::Int16:
      convertCol<clickhouse::ColumnInt16, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::Int32:
      convertCol<clickhouse::ColumnInt32, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::Int64:
      convertCol<clickhouse::ColumnInt64, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::UInt8:
      convertCol<clickhouse::ColumnUInt8, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::UInt16:
      convertCol<clickhouse::ColumnUInt16, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::UInt32:
      convertCol<clickhouse::ColumnUInt32, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::UInt64:
      convertCol<clickhouse::ColumnUInt64, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::Float32:
      convertCol<clickhouse::ColumnFloat32, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::Float64:
      convertCol<clickhouse::ColumnFloat64, Rcpp::IntegerVector>(*this, colIdx, df, start, len);
      break;
    case TC::String:
      convertCol<clickhouse::ColumnString, Rcpp::StringVector>(*this, colIdx, df, start, len);
      break;
    case TC::FixedString:
      convertCol<clickhouse::ColumnFixedString, Rcpp::StringVector>(*this, colIdx, df, start, len);
      break;
    case TC::DateTime:
      convertCol<clickhouse::ColumnDateTime, Rcpp::DatetimeVector>(*this, colIdx, df, start, len);
      break;
    case TC::Date:
      convertCol<clickhouse::ColumnDate, Rcpp::DateVector>(*this, colIdx, df, start, len);
      break;
    default:
      throw std::invalid_argument("unsupported type: "+type->GetName());
      break;
  }
  //TODO: release blocks once they have been fetched
}

void Result::setColInfo(const clickhouse::Block &block) {
  for(clickhouse::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    colNames.push_back(Rcpp::String(bi.Name()));
    colTypes.push_back(bi.Type());
  }
}

bool Result::isComplete() {
  return fetchedRows >= availRows;
}

void Result::addBlock(const clickhouse::Block &block) {
  if(static_cast<size_t>(colNames.size()) < block.GetColumnCount()) {
    setColInfo(block);
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

Rcpp::DataFrame Result::fetchFrame(ssize_t n) {
  size_t nRows = n > 0 ? std::min(static_cast<size_t>(n), availRows-fetchedRows) : availRows-fetchedRows;
  Rcpp::DataFrame df;

  for(size_t i = 0; i < static_cast<size_t>(colNames.size()); i++) {
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
