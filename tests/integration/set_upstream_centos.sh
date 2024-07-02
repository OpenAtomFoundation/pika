sed -i.bak \
  -e 's|^mirrorlist=|#mirrorlist=|g' \
  -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/os/$basearch/|baseurl=http://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/os/$basearch/|g' \
  -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/updates/$basearch/|baseurl=http://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/updates/$basearch/|g' \
  -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/extras/$basearch/|baseurl=http://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/extras/$basearch/|g' \
  -e 's|^#baseurl=http://mirror.centos.org/centos/$releasever/centosplus/$basearch/|baseurl=http://mirrors.tuna.tsinghua.edu.cn/centos/$releasever/centosplus/$basearch/|g' \
  /etc/yum.repos.d/CentOS-Base.repo