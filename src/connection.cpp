// [[Rcpp::plugins(cpp11)]]
// [[Rcpp::interfaces(r, cpp)]]
#define RCPP_NEW_DATE_DATETIME_VECTORS 1
#include <Rcpp.h>
#include <clickhouse/client.h>
#include "result.h"
#include <sstream>

using namespace Rcpp;
using namespace clickhouse;

//' @export
// [[Rcpp::export]]
DataFrame fetch(XPtr<Result> res, ssize_t n) {
  return res->fetchFrame(n);
}

//' @export
// [[Rcpp::export]]
void clearResult(XPtr<Result> res) {
  res.release();
}

//' @export
// [[Rcpp::export]]
bool hasCompleted(XPtr<Result> res) {
  return res->isComplete();
}

//' @export
// [[Rcpp::export]]
size_t getRowCount(XPtr<Result> res) {
  return res->numFetchedRows();
}

//' @export
// [[Rcpp::export]]
size_t getRowsAffected(XPtr<Result> res) {
  return res->numRowsAffected();
}

//' @export
// [[Rcpp::export]]
XPtr<Client> connect(String host, int port, String db, String user, String password, String compression) {
  CompressionMethod comprMethod = CompressionMethod::None;
  if(compression == "lz4") {
    comprMethod = CompressionMethod::LZ4;
  } else if(compression != "" && compression != "none") {
    stop("unknown compression method '"+std::string(compression)+"'");
  }

  Client *client = new Client(ClientOptions()
            .SetHost(host)
            .SetPort(port)
            .SetDefaultDatabase(db)
            .SetUser(user)
            .SetPassword(password)
            .SetCompressionMethod(comprMethod)
            // (re)throw exceptions, which are then handled automatically by Rcpp
            .SetRethrowException(true));
  XPtr<Client> p(client, true);
  return p;
}

//' @export
// [[Rcpp::export]]
void disconnect(XPtr<Client> conn) {
  conn.release();
}

//' @export
// [[Rcpp::export]]
XPtr<Result> select(XPtr<Client> conn, String query) {
  Result *r = new Result;
  //TODO: async?
  conn->Select(query, [&r] (const Block& block) {
    r->addBlock(block);
  });

  XPtr<Result> rp(r, true);
  return rp;
}

template<typename CT, typename RT, typename VT>
void toColumn(SEXP v, std::shared_ptr<CT> col, std::shared_ptr<ColumnUInt8> nullCol,
    std::function<VT(typename RT::stored_type)> convertFn) {
  RT cv = as<RT>(v);
  if(nullCol) {
    for(typename RT::stored_type e : cv) {
      col->Append(convertFn(e));
      nullCol->Append(RT::is_na(e));
    }
  } else {
    for(typename RT::stored_type e : cv) {
      if(RT::is_na(e)) {
        stop("cannot write NA into a non-nullable column of type "+
            col->Type()->GetName());
      }
      col->Append(convertFn(e));
    }
  }
}

template<typename CT, typename VT>
std::shared_ptr<CT> vecToScalar(SEXP v, std::shared_ptr<ColumnUInt8> nullCol = nullptr) {
  auto col = std::make_shared<CT>();
  switch(TYPEOF(v)) {
    case INTSXP: {
      // the lambda could be a default argument of toColumn, but that
      // appears to trigger a bug in GCC
      toColumn<CT, IntegerVector, VT>(v, col, nullCol,
          [](IntegerVector::stored_type x) {return x;});
      break;
    }
    case REALSXP: {
      toColumn<CT, NumericVector, VT>(v, col, nullCol,
          [](NumericVector::stored_type x) {return x;});
      break;
    }
    case LGLSXP: {
      toColumn<CT, LogicalVector, VT>(v, col, nullCol,
          [](LogicalVector::stored_type x) {return x;});
      break;
    }
    default:
      stop("cannot write R type "+std::to_string(TYPEOF(v))+
          " to column of type "+col->Type()->GetName());
  }
  return col;
}

template<>
std::shared_ptr<ColumnDate> vecToScalar<ColumnDate, const std::time_t>(SEXP v,
    std::shared_ptr<ColumnUInt8> nullCol) {
  auto col = std::make_shared<ColumnDate>();
  switch(TYPEOF(v)) {
    case REALSXP: {
      toColumn<ColumnDate, DateVector, const std::time_t>(v, col, nullCol,
          Rf_inherits(v, "POSIXct") ?
            [](DateVector::stored_type x) {return x;} :
            [](DateVector::stored_type x) {return x*(60*60*24);});
      break;
    }
    default:
      stop("cannot write R type "+std::to_string(TYPEOF(v))+
          " to column of type Date");
  }
  for(size_t i = 0; i < col->Size(); i++) {
    Rcout << "entry " << i << ": " << col->At(i) << std::endl;
  }
  return col;
}

template<typename CT, typename VT>
std::shared_ptr<CT> vecToString(SEXP v, std::shared_ptr<ColumnUInt8> nullCol = nullptr) {
  auto col = std::make_shared<CT>();
  switch(TYPEOF(v)) {
    case INTSXP: {
      auto sv = as<StringVector>(v);
      if(nullCol) {
        for(auto e : sv) {
          col->Append(std::string(e));
          nullCol->Append(StringVector::is_na(e));
        }
      } else {
        for(auto e : sv) {
          if(StringVector::is_na(e)) {
            stop("cannot write NA into a non-nullable column of type "+
                col->Type()->GetName());
          }
          col->Append(std::string(e));
        }
      }
      break;
    }
    default:
      stop("cannot write R string of type "+std::to_string(TYPEOF(v))+
          " to column of type "+col->Type()->GetName());
  }
  return col;
}

ColumnRef vecToColumn(TypeRef t, SEXP v, std::shared_ptr<ColumnUInt8> nullCol = nullptr) {
  using TC = Type::Code;
  switch(t->GetCode()) {
    case TC::Int8:
      return vecToScalar<ColumnInt8, int8_t>(v, nullCol);
    case TC::Int16:
      return vecToScalar<ColumnInt16, int16_t>(v, nullCol);
    case TC::Int32:
      return vecToScalar<ColumnInt32, int32_t>(v, nullCol);
    case TC::Int64:
      return vecToScalar<ColumnInt64, int64_t>(v, nullCol);
    case TC::UInt8:
      return vecToScalar<ColumnUInt8, uint8_t>(v, nullCol);
    case TC::UInt16:
      return vecToScalar<ColumnUInt16, uint16_t>(v, nullCol);
    case TC::UInt32:
      return vecToScalar<ColumnUInt32, uint32_t>(v, nullCol);
    case TC::UInt64:
      return vecToScalar<ColumnUInt64, uint64_t>(v, nullCol);
    case TC::Float32:
      return vecToScalar<ColumnFloat32, float>(v, nullCol);
    case TC::Float64:
      return vecToScalar<ColumnFloat64, double>(v, nullCol);
    case TC::String:
      return vecToString<ColumnString, const std::string>(v, nullCol);
    case TC::DateTime:
      return vecToScalar<ColumnDateTime, const std::time_t>(v);
    case TC::Date:
      return vecToScalar<ColumnDate, const std::time_t>(v);
    case TC::Nullable: {
      auto nullCtlCol = std::make_shared<ColumnUInt8>();
      auto valCol = vecToColumn(t->GetNestedType(), v, nullCtlCol);
      return std::make_shared<ColumnNullable>(valCol, nullCtlCol);
    }
    default:
      stop("cannot write unsupported type: "+t->GetName());
  }
}

//' @export
// [[Rcpp::export]]
void insert(XPtr<Client> conn, String tableName, DataFrame df) {
  StringVector names(df.names());
  std::vector<TypeRef> colTypes;

  // determine actual column types
  conn->Select("SELECT * FROM "+std::string(tableName)+" LIMIT 0", [&colTypes] (const Block& block) {
    if(block.GetColumnCount() > 0 && colTypes.empty()) {
      for(ch::Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        colTypes.push_back(bi.Type());
      }
    }
  });

  if(colTypes.size() != static_cast<size_t>(df.size())) {
    stop("input has "+std::to_string(df.size())+" columns, but table "+
        std::string(tableName)+" has "+std::to_string(colTypes.size()));
  }

  Block block;
  for(size_t i = 0; i < colTypes.size(); i++) {
    ColumnRef ccol = vecToColumn(colTypes[i], df[i]);
    block.AppendColumn(std::string(names[i]), ccol);
  }

  conn->Insert(tableName, block);
}

//' @export
// [[Rcpp::export]]
bool validPtr(SEXP ptr) {
  return R_ExternalPtrAddr(ptr);
}
