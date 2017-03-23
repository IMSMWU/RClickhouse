#include <Rcpp.h>
#include <clickhouse/client.h>
#include <sstream>

using namespace Rcpp;
using namespace clickhouse;

// https://ironholds.org/blog/adding-rcpp-to-an-existing-r-package-documented-with-roxygen2/
// Rcpp::sourceCpp('src/connection.cpp')

//' @export
// [[Rcpp::export]]
String getVersion() {
  Client client(ClientOptions().SetHost("localhost"));

  std::stringstream ss;

  client.Select("SELECT uptime() as uptime", [&ss] (const Block& block)
  {
    for (size_t i = 0; i < block.GetRowCount(); ++i) {
      ss << (*block[0]->As<ColumnUInt64>())[i];
    }
  }
  );

  return(ss.str());
}

/*** R
getVersion()
*/
