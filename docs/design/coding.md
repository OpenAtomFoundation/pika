### c++ coding style



### header file

#### Name and order of includes

顺序是

 Related header, C library, C++ library,  other libraries'.h`, your project's `.h`.

不要使用 . 以及 ..  这样的符号

比如一个项目的include 头文件应该是这样

```c++
#include "foo/server/fooserver.h"

#include <sys/types.h>
#include <unistd.h>

#include <hash_map>
#include <vector>

#include "base/basictypes.h"
#include "base/commandlineflags.h"
#include "foo/server/bar.h"

```



### class

#### Doing work in constructors

constructor 不能调用虚函数, 因为在构造的时候, 这个对象还没有完全生成, 因此调用虚函数肯定是不对的



#### Inheritance

使用 override 来表示某一个函数是virtual 函数的重新实现, 这样就不需要在看代码的时候确认这个函数是否是重载, 如果在子类里面对一个父类没有的函数进行override也是会直接报错的

在使用struct的时候 只用在只定义数据,不包含任何方法的结构体里面，出了简单的构造函数或者init函数.

通过使用继承可以有效的减少代码量 并且因为继承是编译时期的 因此在编译器能够识别这些错误  接口继承(也就是定义纯虚函数) 更是能够在编译期就识别一个继承的类是否实现了全部的接口

但是由于继承把一个类的代码分散在各个文件里面了 因此增加了看代码的难度 并且父类定义自己的成员变量 因此访问的时候不是很方便 


所以一定要账号 is-a 和 has-a的关系 一定确定是 a是b的一种的时候才可以使用 继承 **否则尽可能的使用组合** 也就是b里面有一个a的成员变量

### Function

#### parameter Ordering

函数的变量的顺序: input, 然后是output

尽量把一个函数控制在40行以内

#### reference Arguments

所有通过引用传参的变量都需要加上const, 也就是 const type &in

尽可能的input argument 用value 或者 const reference(当然如果这个变量就是指针, 那么传进来的时候就用指针),  然后output argument 用指针

还有就是如果变量需要传进来NULL的时候, 可能会用const T*

#### function overloading

尽可能的不要使用 function overloading, 因为function overloading 增加了c++ 的复杂性. 特别是当继承的时候, 子类只实现了父类的某一个function 的时候, 这样代码的复杂度就更麻烦了. 因为不知道重载的是哪一个函数, 因此

尽可能的不要使用function overloading, 当遇到函数需要不用的变量类型的时候, 可以写成这种AppendString(), AppendInt() 这种

#### default value

允许在非non-virtual 函数里面使用 default value

### scoping

#### Nonmember, static member, global function

如果有一个函数和一个类的对象里面的内容并不相关

那么这个时候有两个选择, 可以定义成class static member function, nonmember function. 那么这个时候如何选择?

如果这个函数和这个对象强相关, 比如是建立一个这个对象, 或者操作这个类的静态成员函数的时候, 将这个函数声明成class static member function 

否则将这个函数声明成nonmember function, 然后用namespace 隔离开来

如果有一个函数只在某一个.cc 文件里面使用, 那么可以将这个函数放在unnamed namespace 或者用static 声明 static int foo() 这种



### other

#### 关于exception 的使用

* pros:
  * exception 可以发现更深层次的错误, 比如a()->b()->c()->d() 那么在d里面抛出的exception 在a里面是可以直接捕获的
  * 比如在c++ 的construction 里面, 我们是无法知道这个construction 是否构造成功, 
* cons
  * ​

#### 关于返回值

1. 在一个函数内部调用



#### brace initializer List

在c++11 里面可以直接通过{} 来初始化一个list, 这个是在c++ 11 之前都不可以的, 比如:

```c++
int main()
{
  std::vector<int> v{1, 2, 3};
  std::map<int, int> mp{{1, 2}, {1, 3}, {1, 4}};
  return 0;
}
```

#### sizeof

在使用sizeof 的时候尽可能的去sizeof(varname), 而不是去sizeof(type).  因此这个varname 随时会更新, 如果varname 这个变量被赋值给其他对象的时候

注意sizeof 的时候考虑对齐的问题

####  Run-Time Type Information(RTTI)

c++ 允许在运行的过程中使用typeid, dynamic_cast 来检查一个变量的类型, 通过dynamic_cast 在类型转换的时候进行检查, 只允许父类的指针指向子类, 而不允许子类的指针指向父类

但是其实用RTTI 的代码都可以用其他的方式来写, 而RTTI 不是很高效, 因此尽可能用 virtual method, 或者 Visitor pattern 模式来实现比较好

#### cast

尽可能的使用 c++ 的static_cast, const_cast, reinterpret_cast 而不是用c 里面的cast

#### stream

如果你为了debug想要打印一个对象内部的细节, 那么经常会提供一个DebugString() 是最经常的

不要使用stream 作为外部用户的IO, stream 性能还是不行的

#### Friend

允许使用 Friend class, function

Friend class 虽然会破坏了类封装, 允许外部类直接访问当前这个类里面的私有成员, 常见的用法就是FooBuilder 应该能够访问Foo 里面的私有成员. 如果没有Friend class, 要么把Foo 的成员都设置成public, 要么给所有的成员变量都添加get, set 函数. 还是很不方便的.

Friend class 只是让某一个类可以访问这个类, 还是比让所有的成员变量都public 来说, 封装更好一些

因此Friend class 需要看到Foo 的私有变量, 因此经常将Friend class 放在同一个头文件里面

#### use of const

能用const 的地方尽可能的使用const

#### Integer type

用<stdint.h> 里面定义的int32\_t, int64\_t 等等, 而不适用short, long, long long 这种类型, 因为short, long 等是根据编译器和平台是不一样的

#### 0 and nullptr/NULL

Use 0 for integers, 0.0 for reals, nullptr (or NULL) for pointers, and '\0' for chars.

在支持c++11 的项目里面尽可能使用nullptr

### Comments

#### TODO comment

写TODO comment 的时候记得写上谁写的这个TODO

// TODO(kl@gmail.com): Use a "*" here for concatenation operator.


### 总结

最后可以用 cpplint.py 跑一下, 尽可能把错误是4, 以及4以上的给排除掉
