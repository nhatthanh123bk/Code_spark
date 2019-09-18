# Các Câu lệnh cơ bản trong hive.

### 1. Tạo bảng trong hive.
- Giả sử cần tạo 1 bảng ratings gồm 4 column userId, movieId, rating, times
  ```SQL
  create table ratings(userId string,movieId string,rating float,times string)
  row format delimited
  fields terminated by ","
  stored as textfile;          
  ```
  Load data vào table từ 1 file csv: 
  ```SQL
  load data (local) inpath 'path/file.csv' (overwrite) into table name_table; 
  ```
### 2. Copy 1 bảng từ 1 bảng khác.
- Giả sử cần tạo 1 bảng ratings của database DB1 từ 1 bảng ratings của database DB2.
  ```SQL
  create table DB1.ratings as select * from DB2.ratings;
  ```
- Nếu chỉ tạo 1 bảng ratings_custom mà chỉ gồm 1 số column của bảng DB1.ratings
  ```SQL
  create table rating_custom(userId string,movieId string,rating float)
  row format delimited
  fields terminated by ","
  stored as textfile;
  ```
  ```SQL
  insert into table DB1.rating_custom(userId,movieId,rating) 
  select userId,movieId,rating from DB2.ratings;
  ```
### 3. Insert 1 số column vào bảng từ 1 số column của bảng khác.
- Giả sử muốn insert data vào column userId của DB1.rating_custom từ userId của DB2.ratings.
  ```SQL
  insert into table DB1.rating_custom(userId,movieId) 
  select userId,movieId from DB2.ratings;
  ```