// [[Rcpp::plugins(cpp11)]]
// [[Rcpp::interfaces(r, cpp)]]
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
XPtr<Client> connect(String host, int port, String db, String user, String password) {
  Client *client = new Client(ClientOptions()
            .SetHost("localhost")
            .SetPort(port)
            .SetDefaultDatabase("default")
            .SetUser("default")
            .SetPassword("")
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
