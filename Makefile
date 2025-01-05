cpp = g++

all:
ifeq ($(shell uname), Linux)
		$(cpp) -I./ -fPIC -shared -o libcppdb.so libcppdb.cpp
endif

install:
ifeq ($(shell uname), Linux)
		$(cpp) -I./ -fPIC -shared -o libcppdb.so libcppdb.cpp
		cp libcppdb.so /usr/lib
		cp libcppdb.hpp /usr/include
endif
