# vn.py框架的DolphinDB数据库管理器

<p align="center">
  <img src ="https://vnpy.oss-cn-shanghai.aliyuncs.com/vnpy-logo.png"/>
</p>

<p align="center">
    <img src ="https://img.shields.io/badge/version-1.0.0-blueviolet.svg"/>
    <img src ="https://img.shields.io/badge/platform-linux|windows-yellow.svg"/>
    <img src ="https://img.shields.io/badge/python-3.7-blue.svg" />
</p>

## 说明

dolphindb数据库接口，需要首先安装DolphinDB 2.0以上版本才能使用，数据读取速度快，适合对数据读取速度有要求用户。

## 安装

下载解压后在cmd运行：

```
python setup.py install
```

## 使用

dolphindb在VN Trader中配置时，需要填写以下字段信息：

| 字段名            | 值 |
|---------           |---- |
|database.driver     | "dolphindb" |
|database.host       | 地址|
|database.port       | 端口|
|database.user       | 用户名| 
|database.password   | 密码| 

 
InfluxDB的例子如下所示：

| 字段名             | 值 |
|---------           |----  |
|database.driver     | dolphindb |
|database.host       | localhost |
|database.port       | 8848 |
|database.user       | admin |
|database.password   | 123456 |

请注意，
windows运行dolphindb.exe的cmd需要保持运行，如果关闭则会导致dolphindb退出，或者也可以使用一些辅助工具将其注册为后台运行的Windows服务。

linux可以切换到下载好的dolphindb文件中server目录，运行以下代码，在后台启动数据库。

```
nohup ./dolphindb -console 0 $
```
