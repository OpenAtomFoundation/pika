nemo本质上是对rocksdb的改造和封装，使其支持多数据结构的存储（rocksdb只支持kv存储）。总的来说，nemo支持五种数据结构类型的存储：KV键值对（为了区分，nemo的的键值对结构用大写的“KV”表示）、Hash结构、List结构、Set结构和ZSet结构。因为rocksdb的存储方式只有kv一种结构，所以以上所说的5种数据结构的存储最终都要落盘到rocksdb的kv存储方式上。

## 1. KV键值对的存储。
KV存储没有添加额外的元信息，只是在value的结尾加上8个字节的附加信息（前4个字节表示version，后 4个字节表示ttl）作为最后落盘kv的值部分。具体如下图：

![](http://ww2.sinaimg.cn/large/c2cd4307jw1f6nbojnvjjj20k204tq30.jpg)
version字段用于对该键值对进行标记，以便后续的处理，如删除一个键值对时，可以在该version进行标记，后续再进行真正的删除，这样可以减少删除操作所导致的服务阻塞时间。


## 2. Hash结构的存储
对于每一个Hash存储，它包括hash键（key），hash键下的域名（field）和存储的值 （value）。nemo的存储方式是将key和field组合成为一个新的key，将这个新生成的key与所要存储的value组成最终落盘的kv键值对。同时，对于每一个hash键，nemo还为它添加了一个存储元信息的落盘kv，它保存的是对应hash键下的所有域值对的个数。下面的是具体的实现方式：
1. 每个hash键、field、value到落盘kv的映射转换
   ![image](http://ww3.sinaimg.cn/large/c2cd4307gw1f6m742p5h5j20rg06iaad.jpg)

2. 每个hash键的元信息的落盘kv的存储格式
   ![](http://ww2.sinaimg.cn/large/c2cd4307jw1f6nbsn0up3j20jx04yweq.jpg)

1. 中前面的横条对应落盘kv的键部分，从前到后，第一个字段是一个字符’h’，表示的是hash结构的key；第二个字段是hash键的字符串长度，用一个字节（uint8_t类型）来表示；第三个字段是hash键的内容，因为第二个字段是一个字节，所以这里限定hash键的最大字符串长度是254个字节；第四个字段是field的内容。a中后面的横条代表的是落盘kv键值对的值部分，和KV结构存储一样，它是存入的value值加上8个字节的version字段和8个字节的ttl字段得到的。
   b中前面的横条代表的存储每个hash键的落盘kv键值对的键部分，它有两字段组成，第一个字段是一个’H’字符，表示这存储时hash键的元信息，第二个字段是对应的hash键的字符串内容；b中后面的横条代表的该元信息的值，它表示对应的hash键中的域值对（field－value）的数量，大小为8个字节（类型是int64_t）。

## 3. List结构的存储
顾名思义，每个List结构的底层存储也是采用链表结构来完成的。对于每个List键，它的每个元素都落盘为一个kv键值对，作为一个链表的一个节点，称为元素节点。和hash一样，每个List键也拥有自己的元信息。

a. 每个元素节点对应的落盘kv存储格式
![image](http://ww4.sinaimg.cn/large/c2cd4307gw1f6m74322gaj20qx06mweu.jpg)

b.每个元信息的落盘kv的存储格式
![image](http://ww2.sinaimg.cn/large/c2cd4307gw1f6m7422myxj20m806mdg6.jpg)

a中前面横条代表的是最终落盘kv结构的键部分，总共4个字段，前面三个字符段分别为一个字符’l’（表明是List结构的结存），List键的字符串长度（1个字节）、List键的字符串内容（最多254个字节），第四个字段是该元素节点所对应的索引值，用8个字节表示（int64_t类型），对于每个元素节点，这个索引（sequence）都是唯一的，是其他元素节点访问该元素节点的唯一媒介；往一个空的List键内添加一个元素节点时，该添加的元素节点的sequence为1，下次一次添加的元素节点的sequence为2，依次顺序递增，即使中间有元素被删除了，被删除的元素的sequence也不会被之后新插入的元素节点使用，这就保证了每个元素节点的sequence都是唯一的。b中后面的横条代表的是具体落盘kv结构的值，它有5个字段，后面的三个字段分别为存入的value值、version、ttl，这和前面的hash结构存储是类似的；前两个字段分别表示的是前一个元素节点的sequence、和后一个元素节点的sequence、通过这两个sequence，就可以知道前一个元素节点和后一个元素节点的罗盘kv的键内容，从而实现了一个双向链表的结构。

b中的前面横条表示存储元信息的落盘kv的键部分，和前面的hash结构是类似的；后面的横条表示存储List键的元信息，它有四个字段，从前到后分别为该List键内的元素个数、最左边的元素 节点的sequence（相当于链表头）、最右边的元素节点的sequence（相当于链表尾）、下一个要插入元素节点所应该使用的sequence。

## 4. Set结构的存储
a.每个元素节点对应的落盘kv存储格式
![image](http://ww4.sinaimg.cn/large/c2cd4307gw1f6m741krdxj20kw06mjrk.jpg)

b.每个Set键的元信息对应的落盘kv存储格式
![image](http://ww3.sinaimg.cn/large/c2cd4307gw1f6m742hinvj20lw06o3yo.jpg)

Set结构的存储和hash结构基本是相同的，只是Set中每个元素对应的落盘kv中，值的部分只有version和ttl，没有value字段。
## 5. ZSet结构的存储
ZSet存储结构是一个有序Set，所以对于每个元素，增加了一个落盘kv，在这个增加的落盘kv的键部分，把该元素对应的score值整合进去，这样便于依据score值进行排序（因为从rocksdb内拿出的数据时按键排序的），下面是落盘kv的存储形式。
a. score值在value部分的落盘kv存储格式
![](http://ww1.sinaimg.cn/large/c2cd4307jw1f6nbzzqa64j20oa068jrk.jpg)

b. score值在key部分的落盘kv存储格式
![](http://ww4.sinaimg.cn/large/c2cd4307jw1f6nc0td9l7j20oa068jrk.jpg)

c.存储元信息的落盘kv的存储格式
![](http://ww2.sinaimg.cn/large/c2cd4307jw1f6nc19p731j20ii068t8u.jpg)

a、c与前面的几种数据结构类似，不再赘述。b中的score是从double类型转变过来的int64_t类型，这样做是为了可以让原来的浮点型的score直接参与到字符串的排序当中（浮点型的存储格式与字符串的比较方式不兼容）。