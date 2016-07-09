# python_data_tools
基于pandas实现的跨多数据源的数据处理与统计工具

## 为什么使用该工具？
### 跨源数据分析与存储需求
我们经常会遇到一下跨源数据统计需求，比如数据A存在于mysql的db1，数据B存在于mysql的db2，数据C存在于excel文件之中。
当我们需要对数据A、B、C进行诸如mysql语句中的按列合并（join）、组合（group by）等等的操作时，那么问题来了。。。跨库数据mysql如何进行操作呢？
python的pandas帮了大忙，其内部的数据结构为DataFrame(数据矩阵)，内建函数merge、concat、group by、desc即相关的数据过滤操作，可以在内存中帮助我们进行这些复杂的数据操作。

### 本工具做了些什么
本工具利用pandas库再更上层进行了封装，包括读写excel文件、读写csv文件、读写mysql文件，并针对pandas原生没有考虑到一些诸如数据库unique key、去重追加、数据覆盖追加的地方进行了补充，并简化了操作方式。
利用pandas，所有的跨源数据全部都会以数据矩阵的方式存储在内存中，使用工具就可以更方便的对数据进行分析、统计、筛选、存取操作，几乎不需要编程（但也支持二次开发）
即配置相关的配置文件（json格式），即可实现数据统计与数据筛选存储的需求。即使是不熟悉python的童鞋，也可以方便的使用。

## 环境依赖
本工具的开发是在python 2.7环境下的，感兴趣的童鞋可以自行尝试对python 3的移植。

### 依赖库及安装方法：
* 如果是linux环境下，需要安装python依赖库（以debian为例）
	+ apt-get install libxml2-dev libxslt1-dev python-dev
	+ apt-get install libmysqlclient-dev
* 数据库操作库
	+ pip install mysql-python sqlalchemy
* 数据统计库
	+ pip install numpy pandas
* excel文件操作库
	+ pip install lxml --upgrade
	+ pip install openpyxl，xlrd

## 如何使用？
1. 将example中的配置文件挪到DataLib文件目录外层
2. 修改对应的配置文件诸如：simple_merge.py
3. 修改配置python文件中
pDeal = DataDealModel('data/', needPrint = False, needInterFile = True)
其中'data/'表示excel文件存取地址，可自定义，目录不存在请自己建立；needPrint参数，表示是否打印出中间数据（内存中），帮助调试；needInterFile参数，表示是否每一次内存数据操作之后，都进行excel存储(False为不存，需最后配置save参数)
4. 参考reference.docx文档,修改其他配置，诸如mysql的db、如何进行merge操作、是否需要数据筛选、存储到哪个文件等等。
5. 执行脚本，诸如: python simple_merge.py 即可