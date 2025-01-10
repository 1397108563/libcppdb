#include <./libcppdb.hpp>

char cppdb::get_column_type(short int column,struct db db){
    char byte;
    short int position = (column-(column%4))/4;
    memcpy(&byte,(char *)db.db_mmap+16+position,1);
    bool byte1=byte&(1<<8-(column%4)*2-1);
    bool byte2=byte&(1<<8-(column%4+1)*2);
    if(!byte1&&byte2){
        return 'c';
    }else if(byte1&&!byte2){
        return 'l';
    }else if(byte1&&byte2){
        return 'd';
    }else{
        return 'e';
    }
}
unsigned long cppdb::get_data_position(unsigned long row_id, short int column_position,struct db db){
    if(db.db_mmap==NULL){
        std::cout << "Database not open" << std::endl;
        return 0;
    }
    if(row_id>db.latest_row_id){
        std::cout << "Row ID out of range" << std::endl;
        return 0;
    }
    if(column_position>db.db_column_count){
        std::cout << "Column ID out of range" << std::endl;
        return 0;
    }
    int column_byte=8;
    for(int i=0; i<column_position-1; i++){
        column_byte+=db.db_column_length[i];
    }
    return 64+(row_id-1)*db.row_size+column_byte;
}
void cppdb::open_db(struct db* db){//内存映射打开数据库
    db->db_size = 64;
    db->fd = open(db->db_path.c_str(),O_RDWR,0666);
    if(db->fd == -1){
        std::cout << "Database not found" << std::endl;
        return;
    }
    void *db_mmap;
    db_mmap = mmap(0,64,PROT_READ|PROT_WRITE,MAP_SHARED,db->fd,0);//将数据库的头64字节部映射到内存中
    if(db_mmap == MAP_FAILED){
        db->db_mmap=NULL;
        return;
    }else{
        db->db_mmap=db_mmap;//写入数据库的详细信息
        unsigned long latest_row_id,db_size,row_size;
        memcpy(&db->db_size,db->db_mmap,8);
        memcpy(&db->row_size,(char *)db->db_mmap+8,8);
        db->latest_row_id = (db->db_size-64)/db->row_size;
        db->db_column_count = 0;
        int char_count = 0;
        for(int i = 0; i < 64; i++){
            char type = cppdb::get_column_type(i,*db);
            if(type == 'l'||type == 'd'){
                db->db_column_count++;
                db->db_column_length[i]=8;
            }else if(type == 'c'){
                if(char_count>16){
                    std::cout << "Too many string/char column" << std::endl;
                    return;
                }
                short int length;
                memcpy(&length,(char *)db->db_mmap+32+char_count*2,2);
                db->db_column_length[i]=length;
                db->db_column_count++;char_count++;
            }else if(type == 'e'){
                break;
            }
            db->db_column_type[i]=type;
        }
        munmap(db->db_mmap,64);//解除映射
        db->db_mmap=mmap(0,db->db_size,PROT_READ|PROT_WRITE,MAP_SHARED,db->fd,0);//重新映射完整的数据库文件
    }
}
void cppdb::close_db(struct db* db){//关闭数据库，解除内存映射并设置指针为0
    munmap(db->db_mmap,db->db_size);
    db->db_mmap=NULL;
}
bool cppdb::write_header(struct db* db){//向新数据库写入头信息
    if(db->db_mmap == 0){
        std::cout << "Database not opened" << std::endl;
        return false;
    }
    unsigned long db_size = 64;
    memcpy(db->db_mmap,&db_size,8);//创建数据库头并写入数据库长度信息
    int j=0;
    for(int i = 0; i < db->db_column_count; i++){//将列类型写入数据库头
        bool new_byte = false;
        if(i%4 == 0){
            new_byte = true;
        }
        if(db->db_column_type[i] == 'c'){//char类型以01(bit)表示
            if(new_byte){
                char byte = 0x40;
                memcpy((char *)db->db_mmap+16+i/4,&byte,1);
            }else{
                char byte = *(char *)((char *)db->db_mmap+16+(i-(i%4))/4);
                byte &= ~(1<<8-(i%4)*2-1);
                byte |= (1<<8-(i%4+1)*2);
                memcpy((char *)db->db_mmap+16+(i-(i%4))/4,&byte,1);
            }
            if(j>=16){
                std::cout << "Too many string/char column" << std::endl;
                return false;
            }
            memcpy((char *)db->db_mmap+32+j*2,db->db_column_length+i,2);
            j++;
        }
        else if(db->db_column_type[i] == 'l'){//long类型以10(bit)表示
            if(new_byte){
                char byte = 0x80;
                memcpy((char *)db->db_mmap+16+i/4,&byte,1);
            }else{
                char byte = *(char *)((char *)db->db_mmap+16+(i-(i%4))/4);
                byte |= (1<<8-(i%4)*2-1);
                byte &= ~(1<<8-(i%4+1)*2);
                memcpy((char *)db->db_mmap+16+(i-(i%4))/4,&byte,1);
            }
        }
        else if(db->db_column_type[i] == 'd'){//double类型以11(bit)表示
            if(new_byte){
                char byte = 0xc0;
                memcpy((char *)db->db_mmap+16+i/4,&byte,1);
            }else{
                char byte = *(char *)((char *)db->db_mmap+16+(i-(i%4))/4);
                byte |= (1<<8-(i%4)*2-1);
                byte |= (1<<8-(i%4+1)*2);
                memcpy((char *)db->db_mmap+16+(i-(i%4))/4,&byte,1);
            }
        }else{
            std::cout << "Invalid column type" << std::endl;
            return false;
        }
    }
    long row_size = 8;//需要加上long类型的行ID
    for(int i = 0; i < db->db_column_count; i++){
        if(db->db_column_type[i] == 'c' || db->db_column_type[i] == 's'){
            row_size += db->db_column_length[i];//char和string类型的自定义长度
            if(db->db_column_type[i] == 'c'&& db->db_column_length[i] ==0 ){
                break;//char类型长度为0表示结束
            }
        }else if(db->db_column_type[i] == 'l'|| db->db_column_type[i] == 'd'){
            row_size += 8;//long和double类型的8字节长度
            db->db_column_length[i] = 8;
        }else{
            std::cout << "Invalid column type" << std::endl;
            return false;
        }
    }
    int k = 0;
    db->row_size = row_size;
    memcpy((char *)db->db_mmap+8,&row_size,8);//写入8-15字节，表示每行的大小
    return true;
}

bool cppdb::create_db(struct db* db){//新建数据库
    db->db_size = 64;
    db->fd = open(db->db_path.c_str(),O_RDWR|O_CREAT,0666);
    int ftr=ftruncate(db->fd,64);
    if(ftr==-1){
        std::cout << "Failed to truncate file" << std::endl;
        return false;
    }
    void *db_mmap;
    db_mmap = mmap(0,64,PROT_READ|PROT_WRITE,MAP_SHARED,db->fd,0);//新数据库长度为固定的64字节
    if(db_mmap == MAP_FAILED){
        db->db_mmap=NULL;
        return false;
    }else{
        db->db_mmap=db_mmap;
    }
    if (!cppdb::write_header(db)){
        return false;
    }//写入数据库头
    db->latest_row_id=0;
    return true;
}

bool cppdb::cppdb_create(struct db* db){//创建数据库的行
    if(db->db_mmap==NULL){
        std::cout << "Database not opened" << std::endl;
        return false;
    }
    cppdb::close_db(db);//关闭数据库
    db->fd = open(db->db_path.c_str(),O_RDWR,0666);
    int ftr=ftruncate(db->fd,db->db_size+db->row_size);
    if(ftr==-1){
        std::cout << "Failed to truncate file" << std::endl;
        return false;
    }
    void *db_mmap;
    db_mmap = mmap(NULL,db->db_size+db->row_size,PROT_READ|PROT_WRITE,MAP_SHARED,db->fd,0);
    if(db_mmap == MAP_FAILED){
        db->db_mmap=NULL;
        return false;
    }
    db->db_mmap=db_mmap;
    db->db_size+=db->row_size;
    unsigned long ID = db->latest_row_id+1;
    memcpy((char *)db->db_mmap+64+(ID-1)*db->row_size,&ID,8);//写入行ID
    memcpy((char *)db->db_mmap,&db->db_size,8);//写入数据库大小
    db->latest_row_id++;
    return true;
}

void cppdb::cppdb_read(void *data,unsigned long position,unsigned long length,struct db db){//从数据库中读取指定位置的数据,data为保存数据的指针
    if(db.db_mmap==NULL){
        std::cout << "Database not opened" << std::endl;
        return;
    }
    if(position>db.db_size-1){
        std::cout << "Position out of range" << std::endl;
        return;
    }
    memcpy(data,(char *)db.db_mmap+position,length);
}

bool cppdb::cppdb_write(const void *data, unsigned long position, unsigned long length,struct db db){//向数据库中写入数据，data为写入的数据指针
    if(data==0){
        std::cout << "Data is 0" << std::endl;
        return false;
    }
    if(position>db.db_size-1){
        std::cout << "Position out of range" << std::endl;
        return false;
    }
    if(length>db.row_size-1){
        std::cout << "Length out of range" << std::endl;
        return false;
    }
    memcpy((char *)db.db_mmap+position,data,length);
    return true;
}

bool cppdb::cppdb_delete(unsigned long ID,struct db* db){//删除数据库中指定行
    if(ID>db->latest_row_id){
        std::cout << "ID out of range" << std::endl;
        return false;
    }
    for(int i=ID; i<db->latest_row_id; i++){
        unsigned long back_ID;
        memcpy(&back_ID,(char *)db->db_mmap+64+i*db->row_size,8);
        back_ID--;
        memcpy((char *)db->db_mmap+64+i*db->row_size,&back_ID,8);
    }
    memmove((char *)db->db_mmap+64+(ID-1)*db->row_size,(char *)db->db_mmap+64+ID*db->row_size,db->db_size-64-ID*db->row_size);
    munmap(db->db_mmap,db->db_size);
    void *db_mmap = mmap(db->db_mmap,db->db_size-db->row_size,PROT_READ|PROT_WRITE,MAP_SHARED,db->fd,0);
    if(db_mmap == MAP_FAILED){
        db->db_mmap=NULL;
        return false;
    }
    db->db_mmap=db_mmap;
    db->db_size=db->row_size-db->row_size;
    db->latest_row_id--;
    return true;
}

unsigned long cppdb::cppdb_search(struct search search,struct db db){//获取数据库中指定数据的位置
    if(search.search_data==NULL){
        std::cout << "Data is 0" << std::endl;
        return 0;
    }
    if(search.search_mode==0){
        std::cout << "Search mode is not set" << std::endl;
        return 0;
    }
    unsigned long position=0;
    if(search.search_mode==1){
        if(search.search_number>db.latest_row_id){
            std::cout << "Search number out of range" << std::endl;
            return 0;
        }
        position=72+db.row_size*(search.search_number-1);
        for(int i=0; i<db.db_column_count; i++){
            if(search.search_type==cppdb::get_column_type(i,db)&&search.search_type=='c'){
                char data[db.db_column_length[i]];
                char search_data[search.search_data_length];
                memcpy(&data,(char *)db.db_mmap+position,db.db_column_length[i]);
                memcpy(&search_data,search.search_data,search.search_data_length);
                if(data==search_data){
                    return position;
                }else{
                    position+=db.db_column_length[i];
                }
            }
            if(search.search_type==cppdb::get_column_type(i,db)&&search.search_type=='l'){
                unsigned long data;
                unsigned long search_data;
                memcpy(&data,(char *)db.db_mmap+position,db.db_column_length[i]);
                memcpy(&search_data,search.search_data,search.search_data_length);
                if(data==search_data){
                    return position;
                }else{
                    position+=db.db_column_length[i];
                }
            }
            if(search.search_type==cppdb::get_column_type(i,db)&&search.search_type=='d'){
                double data;
                double search_data;
                memcpy(&data,(char *)db.db_mmap+position,db.db_column_length[i]);
                memcpy(&search_data,search.search_data,search.search_data_length);
                if(data==search_data){
                    return position;
                }else{
                    position+=db.db_column_length[i];
                }
            }
        }
        return 0;
    }else if(search.search_mode==2){
        if(search.search_number>db.db_column_count){
            std::cout << "Search number out of range" << std::endl;
            return 0;
        }
        position=72;
        for(int i=0;i<search.search_number-1;i++){
            position+=db.db_column_length[i];
        }
        for(int i=0; i<db.latest_row_id; i++){
            if(db.db_column_type[search.search_number-1]=='c'){
                char data[db.db_column_length[search.search_number-1]];
                char search_data[search.search_data_length];
                short int c_int=0;
                memcpy(data,(char *)db.db_mmap+position+i*db.row_size,db.db_column_length[search.search_number-1]);
                memcpy(search_data,search.search_data,search.search_data_length);
                for(int j=0; j<db.db_column_length[search.search_number-1]; j++){
                    if(data[j]!=search_data[j]){
                        break;
                    }else{
                        c_int++;
                    }
                }
                for(int j=0; j<db.db_column_length[search.search_number-1]; j++){
                    data[j]='\0';
                }
                if(c_int==db.db_column_length[search.search_number-1]){
                    return position+i*db.row_size;
                }
            }
            if(db.db_column_type[search.search_number-1]=='l'){
                unsigned long data;
                unsigned long search_data;
                memcpy(&data,(char *)db.db_mmap+position+i*db.row_size,db.db_column_length[search.search_number-1]);
                memcpy(&search_data,search.search_data,search.search_data_length);
                if(data==search_data){
                    return position+i*db.row_size;
                }
            }
            if(db.db_column_type[search.search_number-1]=='d'){
                double data;
                double search_data;
                memcpy(&data,(char *)db.db_mmap+position+i*db.row_size,db.db_column_length[search.search_number-1]);
                memcpy(&search_data,search.search_data,search.search_data_length);
                if(data==search_data){
                    return position+i*db.row_size;
                }
            }
        }
        return 0;
    }else if(search.search_mode==3){
        position=72;
        for(int i=0; i<db.latest_row_id; i++){
            for(int j=0; j<db.db_column_count; j++){
                char data[db.db_column_length[j]];
                memcpy(data,(char *)db.db_mmap+position,db.db_column_length[j]);
                if(data==search.search_data){
                    return position;
                }
                position+=db.db_column_length[j];
            }
            position+=8;
        }
        return 0;
    }else{
        std::cout << "Search mode is nvalid" << std::endl;
        return 0;
    }
}