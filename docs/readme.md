# ekuiper定制开发需求

## 一 注意事项
1. 阅读基准为 ekuiper `1.13.3` tag
2. 调试平台为 `windows` `mingw64` `gcc 11.2.0`
3. ekuiper 最低要求的 golang 版本为 `1.21.0`
4. ekuiper 1.13.3 的代码不能直接在windows mingw64下执行, 严格来讲是不能在windows下执行, 需要修改几处源码才可以, 见备注:
5. make 脚本需要运行于 mingw64 bash下, `make` 编译, `make run` 运行

备注:
> 需要修改的部分
>> `internal\conf\conf.go`
>> ```go
>>  line 313
>>  -- err = LoadConfigFromPath(path.Join(cpath, ConfFileName), &kc)
>>  ++ err = LoadConfigFromPath(filepath.Join(cpath, ConfFileName), &kc)
>> ```
>>
>> `internal\conf\path.go`
>> ```go
>>  line 122
>> -- confDir := path.Join(dir, subdir)
>> ++ confDir := filepath.Join(dir, subdir)
>>
>>  line 130
>> -- confDir = path.Join(dir, subdir)
>> ++ confDir := filepath.Join(dir, subdir)
>> ```
>>
>> `internal\converter\protobuf\converter.go`
>> ```go
>>  line 19
>>  ++ "runtime"
>>
>>  line 37
>>  -- etcDir, _ := kconf.GetLoc("etc/schemas/protobuf/")
>>  -- dataDir, _ := kconf.GetLoc("data/schemas/protobuf/")
>>  -- protoParser = &protoparse.Parser{ImportPaths: []string{etcDir, dataDir}}
>>  ++ 	if runtime.GOOS == "windows" { // TODO: check runtime.GOOS
>>  ++		etcDir, _ := kconf.GetLoc("etc\\schemas\\protobuf\\")
>>  ++		dataDir, _ := kconf.GetLoc("data\\schemas\\protobuf\\")
>>  ++		protoParser = &protoparse.Parser{ImportPaths: []string{etcDir, dataDir}}
>>  ++	} else {
>>  ++		etcDir, _ := kconf.GetLoc("etc/schemas/protobuf/")
>>  ++		dataDir, _ := kconf.GetLoc("data/schemas/protobuf/")
>>  ++		protoParser = &protoparse.Parser{ImportPaths: []string{etcDir, dataDir}}
>>  ++	}
>> ```
>
> 启动时
>> 1. 启动指令 
>>   1.1 mingw bash `export KuiperBaseKey="C:\\Private\\Test\\ekuiper\\" && ./_build/kuiper-1.13.3-windows-amd64/bin/kuiperd`
>>   1.2 Powershell `$env:KuiperBaseKey="E:\Private\Project\Golang\ekuiper\_build\kuiper-1.13.3-1-g9c350505-windows-amd64";E:\Private\Project\Golang\ekuiper\_build\kuiper-1.13.3-1-g9c350505-windows-amd64\bin\kuiperd.exe`
>>   1.3 dos cmd `set KuiperBaseKey=E:\Private\Project\Golang\ekuiper\_build\kuiper-1.13.3-1-g9c350505-windows-amd64&&E:\Private\Project\Golang\ekuiper\_build\kuiper-1.13.3-1-g9c350505-windows-amd64\bin\kuiperd.exe`
>>
>> 2. 启动前 `C:\\Private\\Test\\ekuiper\\` 目录下需要 创建 `etc\\schemas\\protobuf` `data\\schemas\\protobuf` `log` `plugins\\functions` `plugins\\portable` `plugins\\sinks` `plugins\\sources` `plugins\\wasm` 目录
>> 3. 启动前 需要将配置文件放置于 `etc\\kuiper.yaml`
>> 4. cli服务端口 `20498`,  restful 端口 `9081`

目录初始化脚本 `init_dir.bat`
```bat
@echo off

set BASE_DIR=c:\\Private\\Test\\ekuiper

if not exist %BASE_DIR% (
  md %BASE_DIR%
) else (
  rem echo "%BASE_DIR% 文件夹已经存在"
)

cd %BASE_DIR%

if not exist .\\etc\\schemas\\protobuf (
  md .\\etc\\schemas\\protobuf
)

if not exist .\\data\\schemas\\protobuf (
  md .\\data\\schemas\\protobuf
)

if not exist .\\log (
  md .\\log
)

if not exist .\\plugins\\functions (
  md .\\plugins\\functions
)

if not exist .\\plugins\\portable (
  md .\\plugins\\portable
)

if not exist .\\plugins\\sinks (
  md .\\plugins\\sinks
)

if not exist .\\plugins\\sources (
  md .\\plugins\\sources
)

if not exist .\\plugins\\wasm (
  md .\\plugins\\wasm
)

@echo on
```


## 二 阅读笔记


关注点
1. internal/xsql  xsql解析, 依赖`pkg/ast`
2. pkg/ast        xsql的抽象语法树


### 语法解析流程
1. `PlanSQLWithSourcesAndSinks` - `internal\topo\planner\planner.go:45` 的 `func PlanSQLWithSourcesAndSinks(rule *api.Rule, mockSourcesProp map[string]map[string]any, sinks []*node.SinkNode) (*topo.Topo, error)` 函数调用`GetStatementFromSql`解析语法树, 然后调用`createLogicalPlan` 创建plan(`逻辑计划树`), 最后调用`createTopo` 创建topo(`执行算子树`)
2. `GetStatementFromSql` - `internal\xsql\stmtx.go:45` 的 `func GetStatementFromSql(sql string) (stmt *ast.SelectStatement, err error) ` 通过调用`internal\xsql\parser.go:148` `func (p *Parser) Parse() (*ast.SelectStatement, error)` 返回 AST
3. `createLogicalPlan` - `internal\topo\planner\planner.go:460` 的 `func createLogicalPlan(stmt *ast.SelectStatement, opt *api.RuleOption, store kv.KeyValue) (lp LogicalPlan, err error)` 构造与优化逻辑计划
   - 3.1 它接收一个 AST 树对象，并返还一个`逻辑计划树`对象，在整个函数过程中，它总共做了以下 3 件事情:
    - 1. 抽取 SQL 中的各类信息，并将其与实际的表达式或者是 schema 信息进行绑定。
    - 2. 根据 AST 对象构造最初的逻辑计划。
    - 3. 根据最初的逻辑计划进行逻辑优化。
4. `createTopo` - `internal\topo\planner\planner.go:95` 的 `func createTopo(rule *api.Rule, lp LogicalPlan, mockSourcesProp map[string]map[string]any, sinks []*node.SinkNode, streamsFromStmt []string) (t *topo.Topo, err error)` 构建执行算子

> 备注
>> - plan(`逻辑计划树`) 是一条语句的执行计划,  明确语句中各部分逻辑的执行顺序
>> - topo(`执行算子树`) Topo 作为执行算子 Context，会将逻辑计划中的 DataSource 算子放在 sources 中，将其他算子放在 ops 中，而最终的 SQL 结果会汇总到 sinks 中

> [参考](https://www.emqx.com/zh/blog/ekuiper-source-code-interpretation)


### JWT用户认证全流程

见文档[一文讲解JWT用户认证全流程](https://zhuanlan.zhihu.com/p/158186278)

> 暂不开启JWT认证


## 三 功能开发

### 3.1 约定
1. 该平台依赖定制版的 ekuiper v1.13
  - 1.1 新增 `custom_redis` 内置 sink
  - 1.2 新增 `custom_redis` 内置 source
  - 1.3 新增 `custom_redisPub` 内置 sink
  - 1.4 新增 `custom_redisSub` 内置 source, 经过该source的 `time时间戳字段` `精确到ms`
  - 1.5 新增 `聚合函数` `first_value(col, ignoreNull = true)`
  - 1.6 新增 `日期时间函数` `x_timestamp_in_duration(timestamp, start, end)`
  - 1.7 新增 `日期时间函数` `x_from_timestamp(timestamp)`
  - 1.8 新增 `custom_kafkaPub` 内置 sink
  - 1.9 新增 `custom_kafkaSub` 内置 source, 经过该source的 `time时间戳字段` `精确到ms`
  - 1.10 新增 `custom_kafka_2tdb` 内置sink, 经过该sink的数据会被`清洗`或`过滤`后使用`thrift 0.15.0`格式通过`TSDBRpc`平台 `存储`到时序库
  - 1.11 新增 `聚合函数` `get_value(col, index)`  获取指定字段指定索引的值
  - 1.12 新增 `custom_memoryPub` 内置 sink, , 经过该sink的数据会被`清洗`或`过滤`
  - 1.13 新增 `custom_memorySub` 内置 source, 经过该source的 `time时间戳字段` `精确到ms`
2. `custom_redisSub` 处理后的数据格式 见`备注1`


> 备注1: 
> ```json 
>   {
>     "DevCode":  "DTXY:NFFC:0001:Q1", // 资产编码
>     "Metric":   "avg_speed",         // 指标
>     "Time":     1717058649000,       // 时间戳, 精确到ms
>     "DataType": "F",                 // 数据类型
>     "Value":    23.001               // 数据
>   }
> ```

备注2:
> 1. custom_redisSub 接收的数据为 `fieldVal1@type:field2Val@type:cc@field3Val@type` 这样的`:`分割的记录结构; 多条记录之间用`,`分割; `@`分割字段和类型, 可选
> 2. custom_kafkaSub 接收的数据同上
>
> 综上, 字段值和为字符串类型时只可以使用 `[a-zA-Z0-9\_]`表述, 为数值时可以使用 `[0-9\.]`表述

备注3: 
> `custom_kafka_2tdb` 插件必须使用 `thrift 0.15.0`, 而ekuiper自身似乎引用了 `0.19.0`, 可能会产生依赖冲突

> 要点:
>> `时间窗口` 的 `聚合` 会产生 `空值`, 在`custom_kafka_2tdb`插件中会被直接过滤掉, `不写入`时序库, 此特性已经跟 旷文祥 确认

### 3.2 ekuiper规则管理restful api

* 检查规则     POST   http://localhost:9081/rules/validate
* 新增规则     POST   http://localhost:9081/rules
* 展示所有规则 GET    http://localhost:9081/rules 
* 浏览指定规则 GET    http://localhost:9081/rules/{id}
* 修改规则     PUT    http://localhost:9081/rules/{id}
* 删除规则     DELETE http://localhost:9081/rules/{id}
* 查询规则状态 GET    http://localhost:9081/rules/{id}/status
* 启动规则     POST   http://localhost:9081/rules/{id}/start
* 停止规则     POST   http://localhost:9081/rules/{id}/stop
* 重启规则     POST   http://localhost:9081/rules/{id}/restart

#### 3.2.1 数据聚合规则管理

```java
 sql = "SELECT"
    +     "\\\"\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"0\\\" AS Adjust_Sink, "
    +     "${aggregate_type}(Value) AS Value_Sink, "
    +     "window_end() AS Time_Sink "
        + "FROM ${source} "
        + "GROUP BY HOPPINGWINDOW(ss, ${window_time}, ${trigger_cycle}) "
        + " FILTER(WHERE DevCode = \\\"${asset_code}\\\" AND Metric = \\\"${index_code}\\\")"
```

参数绑定
- out_dev_code   输出的资产编码
- out_metric     输出的指标编码
- aggregate_type 聚合函数名称
- source         数据源名称
- window_time    时间窗口大小 单位：秒
- trigger_cycle  触发周期     单位：秒
- asset_code     查询的资产编码
- index_code     查询的指标编码


#### 3.2.2 数据清洗规则管理

概念定义:
* `越限`     处理方式: 删除 或 补点, 补点规则见备注
  - 1. 点的值大于或小于 `指定值`;            
* `跳变`     处理方式: 删除 或 补点, 补点规则见备注
  - 1. 连续两个点的`差的绝对值` `大于或小于` `阈值` ,  `差的绝对值` (最近一个正常点与当前点的差值)
  - 2. 连续两个点的`斜率` (`差的绝对值` / fabs(A点时间戳 - B点时间戳)) `大于或小于` `阈值` 
  - 3. 递增指标的跳变 
* `死值`     处理方式: 删除 或 补点, 补点规则见备注
  - 1. 连续多长时间 `值不变`; 
  - 2. 连续多少个点 `值不变`; 
  - 3. ~~连续多少个点 `值不变` 或 连续多长时间 `值不变`~~; 
  - 4. ~~连续多少个点 `值不变` 且 连续多长时间 `值不变`~~;  
* `时间过滤`  处理方式: 删除 或 补点, 补点规则见备注
  - 1. 事件发生事件为基准, `在`指定时间范围;  
  - 2. 事件发生事件为基准, `不在`指定时间范围, 时间范围格式为`unix时间戳`  


> 补点规则
> 1. 补前一个点
> 2. 补前一段点的平均值 - 可能是补一段时间内的点平均值 或 一批数量的平均值  待定

> 需要输出明文时, sink输出模板: 
> {{range .}},{{.DevCode_Sink}}:{{.Metric_Sink}}@{{.DataType_Sink}}:{{.Value_Sink}}:{{.Time_Sink}}{{end}}

> ~~周新桐说`直接打标签 不补点`~~



##### 3.2.2.1 越限

越限:

```java
 sql = "SELECT"
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "CASE WHEN Value ${than_operator} ${threshold} THEN \\\"1\\\" else \\\"0\\\" END AS Adjust_Sink, "
    +     "${aggregate_type}(Value) AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source} "
    + "WHERE DevCode = \\\"${asset_code}\\\" AND Metric = \\\"${index_code}\\\" AND Value ${than_operator} ${threshold}"
```

非越限:

```java
 sql = "SELECT"
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "CASE WHEN Value ${than_operator} ${threshold} THEN \\\"0\\\" else \\\"1\\\" END AS Adjust_Sink, "
    +     "${aggregate_type}(Value) AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source} "
    + "WHERE DevCode = \\\"${asset_code}\\\" AND Metric = \\\"${index_code}\\\" AND Value ${than_operator} ${threshold}"
```


 - `working_action` action_sink表达式, 见[章节-action_sink表达式格式](####action_sink表达式格式)
 - `out_dev_code`   输出的资产名称
 - `out_metric`     输出的指标名称
 - `adjust_value`   补偿值, 可以是值, 或表达式
 - `aggregate_type` 聚合函数名称
 - `than_operator`  比较符号 `>` `<`
 - `threshold`      阈值

```sql
SELECT
    "proc=out_limit" AS Action_Sink,
    "DTGZJK:BBGF" AS DevCode_Sink,
    "PWhD_C" AS Metric_Sink,
    DataType AS DataType_Sink,
    CASE WHEN Value > 1000 THEN "0" else "1" END AS Adjust_Sink,
    Sum(Value) AS Value_Sink,
    Time AS Time_Sink
FROM custom_redisSub
WHERE DevCode = "DTGZJK:BBGF" AND Metric = "PWhD_C" AND Value > 1000
```


> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=out_limit"`
> 3. 该需求需要`配置2个rule`, 用于处理 `越限` 与 `非越限` 不同sink通道

##### 3.2.2.2 跳变

###### 3.2.2.2.1 ~~连续两个点的`差的绝对值`~~

```java
sql = "SELECT Time, DevCode, Metric, abs(last_value(Value, true) - first_value(Value, true)) AS jumpVal "
     + "FROM ${source}  "
     + "GROUP BY COUNTWINDOW(2, 1) " 
     + "FILTER ( WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
    +            "AND ${rule} )"
```

 - `working_action` action_sink表达式, 见[章节-action_sink表达式格式](####action_sink表达式格式)
 - `out_dev_code`   输出的资产名称
 - `out_metric`     输出的指标名称
 - `adjust_value`   补偿值, 可以是值, 或表达式
 - `aggregate_type` 聚合函数名称
 - `than_operator`  比较符号 `>` `<`
 - `threshold`      阈值


###### 3.2.2.2.2 连续两个点的`斜率`

清洗并补偿跳变记录:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "${threshold} AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
```

```sql
SELECT 
    "proc=jump \n proc.filter=not_in" AS Action_Sink,
    "DTGZJK:BBGF" AS DevCode_Sink,
    "PWhD_C" AS Metric_Sink,
    DataType AS DataType_Sink,
    "5" AS Adjust_Sink,      -- 此处传递阈值
    Value AS Value_Sink
    Time AS Time_Sink
FROM custom_redisSub
WHERE DevCode = "DTGZJK:BBGF" AND Metric = "PWhD_C"

```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=jump \n proc.filter=not_in"`


过滤出跳变记录:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "${threshold} AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=jump \n proc.filter=in"`

###### 3.2.2.2.3 递增指标跳变清洗
指标值持续往一个方向递增, 当前值与前一个值之间的斜率大于阈值, 该需求属于需要特殊处理的跳变, 逻辑独立于普通指标的跳变

条件依据:
1. `当前点`与`上一个点`进行`斜率`计算，若`斜率`大于`阈值`，则认为该点发生`跳变`, 该点被`丢弃`
2. 若跳变状况恢复正常时, 对`当前点`进行`补偿运算`

补偿规则:
- 补偿值: 当前点之前的所有 `跳变点` 与 `前一个点` 之间 `差的总和` 
- `跳变发生后`的`所有`未被丢弃的`正常点`都需要`加`上`补偿值`



```plantext
                12  
              11      ----> 恢复点   11 + 补偿值 
            10      ----> 跳变点  补偿值 (7-10) -3 + -4
        7       ----> 跳变点  补偿值 (3-7) = -4
    3       ----> 正常点
  2
1
```


清洗并补偿跳变记录:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "${threshold} AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=jump \n proc.filter=not_in"`



过滤出跳变记录:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "${threshold} AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=jump \n proc.filter=in"`

##### 3.2.2.3 死值

死值判断的精度精确到 0.000001

###### 3.2.2.3.1 连续多长时间 `值不变`

死值:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"1\\\" AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +     "AND Metric = \\\"${index_code}\\\""
```


非死值:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"0\\\" AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +     "AND Metric = \\\"${index_code}\\\""
```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=dead_time \n proc.count=${window_time}"`
>   - 2.1. 参数 `window_time` 表示连续多少秒无变化
> 3. 该需求需要`配置2个rule`, 用于处理 `死值` 与 `非死值的` 不同sink通道

###### 3.2.2.3.2 连续多少个点 `值不变`

死值:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"1\\\" AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +     "AND Metric = \\\"${index_code}\\\""
```

非死值:
```java
sql = "SELECT "
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"0\\\" AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source}  "
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +     "AND Metric = \\\"${index_code}\\\""
```

> 1. 该需求需要独立的`定制sink`处理, sink名称为 `custom_kafka2Tdb | custom_memoryPub | custom_redis2Tdb`, 该sink直接将数据`写入时序库`
> 2. Action_Sink 固定值为 `"proc=dead_count \n proc.count=${window_count}"`
>   - 2.1 参数 `window_count` 表示连续多少条记录值无变化
> 3. 该需求需要`配置2个rule`, 用于处理 `死值` 与 `非死值的` 不同sink通道

###### 3.2.2.3.3 ~~连续多少个点 `值不变` 或 连续多长时间 `值不变`~~

**无需实现**

###### 3.2.2.3.4 ~~连续多少个点 `值不变` 且 连续多长时间 `值不变`~~

**无需实现**

##### 3.2.2.4 时间过滤

###### 3.2.2.4.1 `在`指定时间范围

```java
 sql = "SELECT"
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "\\\"1\\\" AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source} " 
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
    +            "AND {rule}"
```

- `rule` :  x_timestamp_in_duration(time, ${start}, ${end}) ;  
  - `start` 开始时间 120000 int型 表示 12点整
  - `end`   结束时间 123010 int型 表示 12点30分10秒

> `x_timestamp_in_duration` 判断时间戳是否在指定的闹钟范围内, `左开右开`
> `x_from_timestamp` 自定义函数, 将10位精确到s的时间戳 或 13位精确到ms的时间戳转换位date类型
> Action_Sink 固定值为 `"proc=time_filter \n proc.filter=in"`

###### 3.2.2.4.2 `不在`指定时间范围

```java
 sql = "SELECT"
    +     "\\\"${working_action}\\\" AS Action_Sink, "
    +     "\\\"${out_dev_code}\\\" AS DevCode_Sink, "
    +     "\\\"${out_metric}\\\" AS Metric_Sink, "
    +     "DataType AS DataType_Sink, "
    +     "1 AS Adjust_Sink, "
    +     "Value AS Value_Sink, "
    +     "Time AS Time_Sink "
    + "FROM ${source} " 
    + "WHERE DevCode = \\\"${asset_code}\\\" "
    +            "AND Metric = \\\"${index_code}\\\" "
    +            "AND {rule} != TRUE"
```

- `rule` :  x_timestamp_in_duration(time, ${start}, ${end}) ;  
  - `start` 开始时间 120000 int型 表示 12点整
  - `end`   结束时间 123010 int型 表示 12点30分10秒

> `x_timestamp_in_duration` 判断时间戳是否在指定的闹钟范围内
> `x_from_timestamp` 自定义函数, 将10位精确到s的时间戳 或 13位精确到ms的时间戳转换位date类型
> Action_Sink 固定值为 `"proc=time_filter \n proc.filter=not_in"`

#### 3.2.3 action sink表达式格式

`\n`分割的多行`key`=`value`结构
```ini
proc=jump              # jump|inc_jump|dead|out_limit|time_filter; 当action.type为proc时需要指定
proc.filter=in         # in|not_in
proc.count=${count}    # 数值参数

; proc.func=append       # append|tag
; proc.tag=tag01         # action.proc.func为tag时的tag名
; proc.append.range=prev # prev|next|all|near|custom
; proc.append.calc=avg   # get|avg|sum|abs|add|sub|-sub|
```

##### 3.2.3.1 proc 值描述
- jump        跳变
- inc_jump    递增指标跳变
- dead_time   死值(多长时间无变化)
- dead_count  死值(连续多少个点无变化)
- out_limit   越限
- time_filter 时间过滤

##### 3.2.3.2 proc.filter 值描述
- in     异常值`包含`,  过滤规则
- not_in 异常值`不包含`, 清洗规则

##### 3.2.3.3 ~~proc.append 值描述~~
- prev 前一个
- next 后一个
- all  窗口内所有值, 排除自身
- near prev 和 next
- custom 由定制sink业务处理

##### 3.2.3.4 ~~proc.append.calc 值描述~~
- get  取值
- avg  平均值
- sum  累加和
- abs  绝对值
- add  prev + next
- sub  prev - next
- -sub next - prev