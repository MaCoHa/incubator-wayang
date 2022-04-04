from typing import Iterable

from pywayang.platform import Platform
from pywayang.context import WayangContext
from pywayang.plugin import java, spark
from pywayang.operator.unary import *

p = Platform("nana")
print(p)


print(str(WayangContext().register(java, spark)))

from pywayang.types import Predicate, getTypePredicate

predicate : Predicate = lambda x: x % 2 == 0
getTypePredicate(predicate)

def pre(a:str):
    return len(a) > 3

def func(s:str) -> int:
    return len(s)

def fmfunc(i:int) -> str:
    for x in range(i):
        yield str(x)

fileop = WayangContext()\
            .register(java)\
            .textFile("here")\

filterop: FilterOperator = fileop.filter(pre).getOperator()
fop_pre = filterop.getWrapper()
fop_pre_res = fop_pre(["la", "lala"])
#for i in fop_pre_res:
#    print(i)


mapop: MapOperator = fileop.map(func).getOperator()
mop_func = mapop.getWrapper()
mop_func_res = mop_func(["la", "lala"])
#for i in mop_func_res:
#    print(i)


fmop: FlatmapOperator = fileop.flatmap(fmfunc).getOperator()
fmop_func = fmop.getWrapper()
fmop_func_res = fmop_func([2, 3])
#for i in fmop_func_res:
#    print(i)

def concatenate(function_a, function_b):
    def executable(iterable):
        return function_b(function_a(iterable))
    return executable

res = concatenate(concatenate(fop_pre, mop_func), fmop_func)
res_pro = res(["la", "lala"])
for i in res_pro:
    print(i)