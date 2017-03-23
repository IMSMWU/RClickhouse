

#test_con <- src_clickhouse(host="db-imsm.wu.ac.at", port = 31010L)



suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
})



my_db <- src_sqlite("my_db.sqlite3", create = T)

library(nycflights13)
flights_sqlite <- copy_to(my_db, flights, temporary = FALSE, indexes = list(
  c("year", "month", "day"), "carrier", "tailnum"))


tbl(nycflights13_sqlite(), "flights") %>% mutate (n = count())

test_con <- src_clickhouse(host="172.17.0.5", port = 8123L)
test_con

%>% tbl("itunes")



test_con %>% tbl("airports")

k <- test_con %>% tbl("itunes") %>% group_by(country, yeaer, month, day) %>%
  summarize(csum=sum(units)) %>% as.data.frame()


test_con %>% tbl("youtube") %>% mutate(n = n())

sql_render(k)

sql <- test_con %>% tbl("youtube") %>% mutate(bla=1)
sql_render(sql)

sql


%>% select(n)




test_con %>% tbl("youtube") %>% head(5)


## test DBI
## db_query_fields: which should return a character vector giving the field names of a query
db_query_fields(test_con$con, "SELECT  FROM videos")



db_query_fields(test_con$con, dplyr::sql("SELECT 1 AS a, 'text' AS b"))



fields <- dplyr::db_query_fields(
  test_con$con,
  dplyr::sql_subquery(
    test_con$con,
    dplyr::sql("SELECT 1 AS a, 'text' AS b")
  )
)


con <- test_con$con
sql <- dplyr::sql("SELECT 1 AS `a`, 'text' AS `b`")

fields <- dplyr::build_sql(
  "SELECT * FROM ", dplyr::sql_subquery(con, sql), " LIMIT 1",
  con = con
)

fields <- dplyr::build_sql("SELECT * FROM (",sql,") LIMIT 1", con = con)

fields





k <- test_con %>% tbl("youtube") %>% head(1)
k

k %>% as.data.frame()
test_con %>% tbl("youtube") %>% as.data.frame()

k <- test_con %>% tbl("youtube") %>% transmute(n = n())

# SELECT * FROM `youtube`

sql_render(k)
