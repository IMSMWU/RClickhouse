#include <stdexcept>

#include "result.h"

// helper function for converting the column data (can't be a member of Result
// due to C++ prohibiting explicit specialization on members of a
// non-specialized class)
template<typename CT, typename RT>
inline void convertCol(const Result &r, Result::AccFunc colAcc, Rcpp::DataFrame &df,
    size_t start, size_t len, Result::AccFunc nullAcc) {
  r.convertTypedColumn<CT, RT>(colAcc, df, start, len,
    [&nullAcc](const Result::ColBlock &cb, std::shared_ptr<const CT> in, RT &out,
        size_t offset, size_t start, size_t end) {
      for(size_t j = start; j < end; j++) {
        if(nullAcc) {
          std::shared_ptr<ch::ColumnNullable> nullCol = nullAcc(cb)->As<ch::ColumnNullable>();
          // can't use the ternary operator here, since that would require explicit
          // conversion from the Clickhouse storage type (which is far messier)
          if(nullCol->IsNull(j)) {
            out[offset+j-start] = RT::get_na();
          } else {
            out[offset+j-start] = in->At(j);
          }
        } else {
          out[offset+j-start] = in->At(j);
        }
      }
    }
  );
}

// Date requires specialization: otherwise causes problems due to type
// ambiguities in the Rcpp::Date constructor, which expects either int or
// double, whereas ColumnDate values are uint32_t
template<>
inline void convertCol<ch::ColumnDate, Rcpp::DateVector>(const Result &r,
    Result::AccFunc colAcc, Rcpp::DataFrame &df, size_t start, size_t len,
    Result::AccFunc nullAcc) {
  using CT = ch::ColumnDate;
  using RT = Rcpp::DateVector;
  r.convertTypedColumn<CT, RT>(colAcc, df, start, len,
    [&nullAcc](const Result::ColBlock &cb, std::shared_ptr<const CT> in, RT &out,
        size_t offset, size_t start, size_t end) {
      for(size_t j = start; j < end; j++) {
        if(nullAcc) {
          std::shared_ptr<ch::ColumnNullable> nullCol = nullAcc(cb)->As<ch::ColumnNullable>();
          out[offset+j-start] = nullCol->IsNull(j) ? RT::get_na() : static_cast<int>(in->At(j)/(60*60*24));
        } else {
          out[offset+j-start] = static_cast<int>(in->At(j)/(60*60*24));
        }
      }
    }
  );
}

template<typename CT, typename RT>
void Result::convertTypedColumn(AccFunc colAcc, Rcpp::DataFrame &df,
    size_t start, size_t len,
    ConvertFunc<CT, RT> convFunc) const {
  RT v(len);   // R vector for the column

  size_t i = 0;
  for(const ColBlock &cb : columnBlocks) {
    const ch::ColumnRef col = colAcc(cb);
    if(i+col->Size() >= start) {  // only if this block was not fetched yet
      auto ccol = col->As<CT>();

      // first index within this block (note: can't use std::max(start-i, 0)
      // here, since the variables are unsigned, and i>start is possible)
      size_t localStart = (i < start ? start-i : 0),
      // one past the last index within this block
      // guaranteed to be >=0, since the loop is aborted if i >= start+len
             localEnd = std::min(start+len-i, col->Size());

      convFunc(cb, ccol, v, i, localStart, localEnd);
    }

    i += col->Size();
    if(i >= start+len) {  // processed all blocks in the requested range
      break;
    }
  }
  df.push_back(v);
}

void Result::convertColumn(AccFunc colAcc, TypeAccFunc typeAcc, Rcpp::DataFrame &df,
    size_t start, size_t len, AccFunc nullAcc) const {
  using TC = ch::Type::Code;
  auto type = typeAcc(colTypes);
  switch(type->GetCode()) {
    case TC::Int8:
      convertCol<ch::ColumnInt8, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Int16:
      convertCol<ch::ColumnInt16, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Int32:
      convertCol<ch::ColumnInt32, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Int64:
      convertCol<ch::ColumnInt64, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::UInt8:
      convertCol<ch::ColumnUInt8, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::UInt16:
      convertCol<ch::ColumnUInt16, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::UInt32:
      convertCol<ch::ColumnUInt32, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::UInt64:
      convertCol<ch::ColumnUInt64, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Float32:
      convertCol<ch::ColumnFloat32, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Float64:
      convertCol<ch::ColumnFloat64, Rcpp::IntegerVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::String:
      convertCol<ch::ColumnString, Rcpp::StringVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::FixedString:
      convertCol<ch::ColumnFixedString, Rcpp::StringVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::DateTime:
      convertCol<ch::ColumnDateTime, Rcpp::DatetimeVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Date:
      convertCol<ch::ColumnDate, Rcpp::DateVector>(*this, colAcc, df, start, len, nullAcc);
      break;
    case TC::Nullable: {
      convertColumn([&colAcc](const ColBlock &cb){return colAcc(cb)->As<ch::ColumnNullable>()->Nested();},
          [&typeAcc](const TypeList &tl){return typeAcc(tl)->GetNestedType();},
          df, start, len,
          [&colAcc](const ColBlock &cb){return colAcc(cb)->As<ch::ColumnNullable>();});
      break;
    }
    default:
      throw std::invalid_argument("unsupported type: "+type->GetName());
      break;
  }
}

void Result::setColInfo(const ch::Block &block) {
  for(ch::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    colNames.push_back(Rcpp::String(bi.Name()));
    colTypes.push_back(bi.Type());
  }
}

bool Result::isComplete() const {
  return fetchedRows >= availRows;
}

size_t Result::numFetchedRows() const {
  return fetchedRows;
}

size_t Result::numRowsAffected() const {
  return 0;   //TODO
}

void Result::addBlock(const ch::Block &block) {
  if(static_cast<size_t>(colNames.size()) < block.GetColumnCount()) {
    setColInfo(block);
  }

  if(block.GetRowCount() > 0) {   // don't add empty blocks
    ColBlock cb;
    for(ch::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
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
    convertColumn([&i](const ColBlock &cb){return cb.columns[i];},
        [&i](const TypeList &tl){return tl[i];}, df, fetchedRows, nRows);
  }

  df.attr("class") = "data.frame";
  if(nRows > 0) {
    df.attr("row.names") = Rcpp::Range(fetchedRows+1, fetchedRows+nRows);
  }
  df.attr("names") = colNames;
  fetchedRows += nRows;

  return df;
}
