# SSIS_Util
extract connection, SQL from SSIS(SQL Server Integration Service ) package file(.dtsx)

## usage   
python ssis_util.py --in_path .\in --out_path .\out   

## output sample SQL
```SQL
/* Precedence Constraint(실행 순서)
[Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task1] --> [Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task2]
[Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task2] --> [Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task3]
[Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task3] --> [Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task4]
[Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task4] --> [Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task5]
*/


/******************************************************************************************************/


/* Connections(연결 정보)
1. key 캐시연결
  - CreationName:CACHE
  - ConnectionString:[]

2. 10.x.x.1.admin.admin
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;User ID=admin;Initial Catalog=admin;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]

3. 10.x.x.1.db3.admin
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;User ID=admin;Initial Catalog=db3;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]

4. 10.x.x.1.db3.admin
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;Initial Catalog=db4;Provider=SQLNCLI11;Integrated Security=SSPI;]

5. localhost.db2
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.2;User ID=admin;Initial Catalog=db2;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]

6. localhost.db1
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.3;User ID=admin;Initial Catalog=db1;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]
*/


/******************************************************************************************************/

/* [Data Flow(데이터 흐름) TaskName: Package\update\task4 Source]
   [Connection: Package.ConnectionManagers[10.x.x.4.db6.admin]] */
select ...
  from ...

/******************************************************************************************************/

/* [Data Flow(데이터 흐름) TaskName: Package\update\task4 Target]
   [Connection: Package.ConnectionManagers[localhost.db1]] */
OpenRowset: [dbo].[table1]

/******************************************************************************************************/


/* [Control Flow(제어 흐름) TaskName: Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task1]
   [Connection: 6. localhost.db1
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.3;User ID=admin;Initial Catalog=db1;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]]
*/
delete from ... where dt = @dt

insert ...
(...)
select
...
from (
    select 
        ...
    from #tempp a
        inner join ... b
            on ...
    where (...)
) t
where ...

/******************************************************************************************************/

/* [Control Flow(제어 흐름) TaskName: Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task2]
   [Connection: 5. localhost.db2
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.2;User ID=admin;Initial Catalog=db2;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]]
*/
if object_id('tempdb.dbo.#tempp') is not null
    drop table #tempp

select
     ...
  into #tempp
  from ... a
 where dt = @dt
 group by ...

merge ... a
    using (...) b
        on ...
when not matched then 
    insert(...)
    values(...);

/******************************************************************************************************/

/* [Control Flow(제어 흐름) TaskName: Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task3]
   [Connection: 3. localhost.db3
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;User ID=admin;Initial Catalog=db3;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]]
*/

update b
   set ...
  from ... a
       inner join ... b
        on ...
where ...

/******************************************************************************************************/

/* [Control Flow(제어 흐름) TaskName: Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task4]
   [Connection: 4. localhost.db4
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;Initial Catalog=db4;Provider=SQLNCLI11;Integrated Security=SSPI;]]
*/
update b
   set ...
  from ... a
       inner join ... b
        on ...
where ...

/******************************************************************************************************/

/* [Control Flow(제어 흐름) TaskName: Package\Foreach 루프 컨테이너\시퀀스 컨테이너\task5]
   [Connection: 2. localhost.admin
  - CreationName:OLEDB
  - ConnectionString:[Data Source=10.x.x.1;User ID=admin;Initial Catalog=admin;Provider=SQLNCLI11;Persist Security Info=True;Auto Translate=False;]]
*/
if object_id('tempdb.dbo.#tempm') is not null
    drop table #tempm

select
      ...
  into #tempm
  from a
 where a.dt = @dt

merge ... a
using (...) b
   on ...
  and ...
when not matched then
    insert (...)
    values (...);

/******************************************************************************************************/

```
