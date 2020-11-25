#include <map>
#include <string>
#include "ydb_server_occ.h"
#include "extent_client.h"
#include "extent_protocol.h"

//#define DEBUG 1

ydb_server_occ::ydb_server_occ(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst), next_id(0) {
}

ydb_server_occ::~ydb_server_occ() {
}


ydb_protocol::status ydb_server_occ::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    out_id = next_id++;
    active_ids.insert(out_id);

    lc->release(EID_MAX);
    return ret;
}

ydb_protocol::status ydb_server_occ::transaction_commit(ydb_protocol::transaction_id id, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;
	
    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    for (const auto &kv : read_set[id]) {
        std::string buf;
        extent_protocol::extentid_t eid = hash(kv.first);
        std::map<std::string, std::string> kvmap;

        if (ec->get(eid, buf) != extent_protocol::OK) {
            ret = ydb_protocol::RPCERR;
            goto release;
        }

        kvmap = parse_file_content(buf);
        if (kvmap[kv.first] != kv.second) {
            active_ids.erase(id);
            read_set.erase(id);
            write_set.erase(id);
            ret = ydb_protocol::ABORT;
            goto release;
        }
    }

    for (const auto &kv : write_set[id]) {
        std::string buf;
        extent_protocol::extentid_t eid = hash(kv.first);
        std::map<std::string, std::string> kvmap;

        while (ec->get(eid, buf) != extent_protocol::OK);

        kvmap = parse_file_content(buf);
        kvmap[kv.first] = kv.second;

        buf = construct_file_content(kvmap);
        while (ec->put(eid, buf) != extent_protocol::OK);
    }

    active_ids.erase(id);
    read_set.erase(id);
    write_set.erase(id);

release:
    lc->release(EID_MAX);
    return ret;
}

ydb_protocol::status ydb_server_occ::transaction_abort(ydb_protocol::transaction_id id, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    active_ids.erase(id);
    read_set.erase(id);
    write_set.erase(id);

release:
    lc->release(EID_MAX);
    return ret;
}

ydb_protocol::status ydb_server_occ::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (write_set[id].count(key)) {
        out_value = write_set[id][key];
    } else if (read_set[id].count(key)) {
        out_value = read_set[id][key];
    } else {
        std::string buf;
        extent_protocol::extentid_t eid = hash(key);
        std::map<std::string, std::string> kvmap;
        
        if (ec->get(eid, buf) != extent_protocol::OK) {
            ret = ydb_protocol::RPCERR;
            goto release;
        }

        kvmap = parse_file_content(buf);
        out_value = kvmap[key];

        read_set[id][key] = out_value;
    }

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_occ::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    write_set[id][key] = value;

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_occ::del(ydb_protocol::transaction_id id, const std::string key, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    write_set[id][key] = "";

release:
    lc->release(EID_MAX);
	return ret;
}

