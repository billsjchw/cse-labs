#include <string>
#include <utility>
#include "ydb_server.h"
#include "extent_client.h"
#include "extent_protocol.h"

//#define DEBUG 1

static long timestamp(void) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec*1000 + tv.tv_usec/1000);
}

ydb_server::ydb_server(std::string extent_dst, std::string lock_dst) {
	ec = new extent_client(extent_dst);
	lc = new lock_client(lock_dst);
	//lc = new lock_client_cache(lock_dst);

	long starttime = timestamp();
	
	for (extent_protocol::extentid_t i = EID_MIN; i < EID_MAX; i++) {    // for simplicity, just pre alloc all the needed inodes
		extent_protocol::extentid_t id;
		ec->create(extent_protocol::T_FILE, id);
	}
	
	long endtime = timestamp();
	printf("time %ld ms\n", endtime-starttime);
}

ydb_server::~ydb_server() {
	delete lc;
	delete ec;
}


ydb_protocol::status ydb_server::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::get(ydb_protocol::transaction_id id, std::string key, std::string &out_value) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;

    lc->acquire(eid);
    
    if (ec->get(eid, buf) != extent_protocol::OK) {
        lc->release(eid);
        return ydb_protocol::RPCERR;
    }
    
    lc->release(eid);

    kvmap = parse_file_content(buf);
    out_value = kvmap[key];

    return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::set(ydb_protocol::transaction_id id, std::string key, std::string value, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;

    lc->acquire(eid);

    if (ec->get(eid, buf) != extent_protocol::OK) {
        lc->release(eid);
        return ydb_protocol::RPCERR;
    }

    kvmap = parse_file_content(buf);
    kvmap[key] = value;

    buf = construct_file_content(kvmap);
    if (ec->put(eid, buf) != extent_protocol::OK) {
        lc->release(eid);
        return ydb_protocol::RPCERR;
    }

    lc->release(eid);

    return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::del(ydb_protocol::transaction_id id, std::string key, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;

    lc->acquire(eid);

    if (ec->get(eid, buf) != extent_protocol::OK) {
        lc->release(eid);
        return ydb_protocol::RPCERR;
    }

    kvmap = parse_file_content(buf);
    kvmap.erase(key);

    buf = construct_file_content(kvmap);
    if (ec->put(eid, buf) != extent_protocol::OK) {
        lc->release(eid);
        return ydb_protocol::RPCERR;
    }

    lc->release(eid);

    return ydb_protocol::OK;
}

extent_protocol::extentid_t ydb_server::hash(const std::string &key) {
    unsigned c = 0;
    unsigned h = 5381;
    const char *ptr = NULL;

    ptr = key.c_str();
    while ((c = *ptr++))
        h = ((h << 5) + h) + c;

    return (h % (EID_MAX - EID_MIN)) + EID_MIN;
}

std::string ydb_server::construct_file_content(const std::map<std::string, std::string> &kvmap) {
    std::string file_content;

    for (const auto &kv : kvmap) {
        std::size_t key_len = kv.first.size();
        std::size_t value_len = kv.second.size();
        file_content.append((char *) &key_len, 1);
        file_content.append((char *) &value_len, 1);
        file_content += kv.first;
        file_content += kv.second;
    }

    return file_content;
}

std::map<std::string, std::string> ydb_server::parse_file_content(const std::string &file_content) {
    std::map<std::string, std::string> kvmap;
    std::size_t file_size = file_content.size();

    for (std::size_t i = 0; i < file_size;) {
        std::size_t key_len = file_content[i];
        std::size_t value_len = file_content[i + 1];
        std::string key = file_content.substr(i + 2, key_len);
        std::string value = file_content.substr(i + 2 + key_len, value_len);
        kvmap[key] = value;
        i += 2 + key_len + value_len;
    }

    return kvmap;
}
