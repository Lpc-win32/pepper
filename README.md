# pepper

基于libpc协程库的RPC框架，用于便捷开发Linux端高并发网络服务程序。

## pepper框架说明

Linux C++(C++11)

计划pepper框架中包含如下模块：

- 底层协程库: libpc (ok)
- 服务发现: etcd client
- 协议双向兼任: proto buffer
- 缓存数据库: redis client (ok)
- 数据库: mysql client
- http服务: fast cgi
- 项目运行: 前台/后台
- 日志管理: libpc已实现
- 配置文件解析: pcl (ok)
- RPC模块
- 消息队列中间件: RabbitMQ Client

## 目前已实现

- libpc
- pcl
- logger

## 额外的说明

- Lpc-Win32 HomePage : http://www.lpc-win32.com
- Lpc-Win32 Blog : http://blog.lpc-win32.com
