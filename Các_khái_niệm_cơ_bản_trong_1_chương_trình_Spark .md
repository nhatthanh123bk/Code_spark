# Các khái niệm cơ bản trong 1 chương trình Spark

## 1. Partition.
- 1 RDDs hay Dataframe sẽ được chia thành nhiều phần nhỏ sau đó phân tán vào các executor để thực hiện tính toán song song, mỗi phần như vậy gọi là 1 partition.
- Thông thường khi 1 RDDs hay Dataframe vừa mới tạo ra sẽ tự động được chia thành các partition. Có thể kiểm tra số partition của 1 RDD hay Dataframe bằng câu lệnh sau:
  
    ```Python
    RDD.getNumPartitions() // với RDDs
    df.rdd.getNumPartitions() // với dataframe
    ``` 
- Ta có thể  partition lại bằng câu lệnh:
    ```Python
    RDD.repartition(number_of_partition,"name_column")
    df.rdd.repartition(number_of_partition,"name_column")
    ```
- Kích thước lớn nhất của 1 partion đã được config và thường sẽ có kích thước tối đa là 64mb hoặc 128mb tùy theo phiên bản Spark mà bản sử dụng.    
- Việc RDDs và Dataframe được chia thành bao nhiều partition sẽ có ảnh hưởng lớn đến hiệu suất tính toán của Spark. Việc turning số partition sẽ được nói trong phần sau, khi ta hiểu được hết 1 luồng chương trình Spark được vận hành như thế nào.
## 2. Job