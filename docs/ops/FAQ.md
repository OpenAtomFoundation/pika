### 1 Compile and Install

#### Q1: OS
A1： Only support Linux(Centos and Ubuntu)

#### Q2: How to?
A2： Refer to [wiki](install.md)

#### Q3： Error message `isnan isinf was not declared` in Ubuntu？
A3： Replace `std::isnan` and `std::isinf` with `isnan`，`isinf`, include `cmath`
```
#include <cmath>
```
