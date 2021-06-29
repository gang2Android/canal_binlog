## canal_binlog
go-监控binlog并发送到mq中

### 运行

- 修改main.go中的mq参数与数据库的参数
- go run main.go

### 打Linux包

- 构建
  ```shell
  set GOOS=linux
  set GOARCH=amd64
  go build -ldflags "-s -w" -o canal_binlog
  ```
- 上传服务器
- 执行
  ```shell
  # 上传到服务器目录下
  chmod 777 canal_binlog
  nohup ./canal_binlog &
  ```
