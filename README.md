# The plugin of agent for blaze


#### install:

```shell
pip install git+https://github.com/sandabuliu/blaze-agent.git
```
or      
```shell
git clone https://github.com/sandabuliu/blaze-agent.git
cd blaze-agent
python setup.py install
```

#### example:

```python
from agent import rule
from blaze import data, compute, by

df = data('agent:/var/log/nginx/access.log', rule=rule('nginx'))
print df.peek()                                                         #  预览数据
print compute(df.count(), chunks=2, chunksize=2000)                     #  计算两个窗口内的个数, 每个窗口大小为 2000
print compute(df.method.distinct().count(), chunks=1, chunksize=10000)  #  计算一个窗口内的请求方法种类, 每个窗口大小为10000
print compute(df.body_bytes_sent.max())                                 #  计算最大发送包大小
print compute(by(df.remote_addr, count=df.count()))                     #  统计不同IP的访问量 
```