#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

struct search{//搜索结构体
    /*mode 1: 搜索一行数据
      mode 2: 搜索一列数据
      mode 3: 全部搜索*/
    short int search_mode=0;
    /*c,s,l,d,如果是按列搜索或全部搜索则无需指定*/
    char search_type;
    /*搜索的行号或列号,如果是模式3可忽略*/
    unsigned long search_number=0;
    //需要搜索的数据
    void *search_data;
    /*搜索的数据长度,如果是模式3可忽略*/
    short int search_data_length;
};

struct db{//数据库信息结构体
    std::string db_path;
    int fd;
    void *db_mmap=NULL;
    unsigned long db_size;
    int db_column_count;
    char db_column_type[64]={'c'};
    short int db_column_length[64]={0};
    unsigned long latest_row_id;
    unsigned long row_size;
};

class cppdb{
    public:
        std::string db_path;
        void cppdb_read(void *data,unsigned long position,unsigned long length,struct db);
        bool cppdb_write(const void *data, unsigned long position, unsigned long length,struct db);
        unsigned long cppdb_search(struct search,struct db);
        bool cppdb_delete(unsigned long ID,struct db*);
        bool cppdb_create(struct db*);
        bool create_db(struct db*);
        void open_db(struct db*);
        void close_db(struct db*);
        unsigned long get_data_position(unsigned long row_id, short int column_position,struct db);
    private:
        bool write_header(struct db*);
        char get_column_type(short int position,struct db);
};