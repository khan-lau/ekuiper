# 阅读笔记

## 注意事项
1. 阅读基准为 ekuiper `1.13.3` tag
2. 调试平台为 `windows` `mingw64` `gcc 11.2.0`
3. ekuiper 最低要求的 golang 版本为 `1.21.0`
4. ekuiper 1.13.3 的代码不能直接在windows mingw64下执行, 严格来讲是不能在windows下执行, 需要修改几处源码才可以, 见备注:


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
>> 1. 启动指令 `export KuiperBaseKey="C:\\Private\\Test\\ekuiper\\" && ./_build/kuiper-1.13.3-windows-amd64/bin/kuiperd`
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


## 阅读笔记


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
