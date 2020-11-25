#include "ydb_server_2pl.h"
#include "extent_client.h"
#include <map>

//#define DEBUG 1

ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst), next_id(0) {
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	ydb_protocol::status ret = ydb_protocol::OK;
    
    lc->acquire(EID_MAX);

    out_id = next_id++;
    active_ids.insert(out_id);

    lc->release(EID_MAX);
    return ret;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	ydb_protocol::status ret = ydb_protocol::OK;
    
    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    for (extent_protocol::extentid_t eid : lock_set[id])
        lc->release(eid);

    active_ids.erase(id);
    lock_set.erase(id);
    log.erase(id);

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;
    
    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    for (const auto &kv : log[id]) {
        std::string buf;
        extent_protocol::extentid_t eid = hash(kv.first);
        std::map<std::string, std::string> kvmap;

        if (ec->get(eid, buf) != extent_protocol::OK) {
            ret = ydb_protocol::RPCERR;
            goto release;
        }

        kvmap = parse_file_content(buf);
        kvmap[kv.first] = kv.second;

        buf = construct_file_content(kvmap);
        if (ec->put(eid, buf) != extent_protocol::OK) {
            ret = ydb_protocol::RPCERR;
            goto release;
        }
    }

    for (extent_protocol::extentid_t eid : lock_set[id])
        lc->release(eid);

    active_ids.erase(id);
    lock_set.erase(id);
    log.erase(id);

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, std::string key, std::string &out_value) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        lc->acquire(eid);
        lock_set[id].insert(eid);
    }

    if (ec->get(eid, buf) != extent_protocol::OK) {
        ret = ydb_protocol::RPCERR;
        goto release;
    }

    kvmap = parse_file_content(buf);
    out_value = kvmap[key];

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, std::string key, std::string value, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        lc->acquire(eid);
        lock_set[id].insert(eid);
    }

    if (ec->get(eid, buf) != extent_protocol::OK) {
        ret = ydb_protocol::RPCERR;
        goto release;
    }

    kvmap = parse_file_content(buf);
    if (!log[id].count(key))
        log[id][key] = kvmap[key];
    kvmap[key] = value;

    buf = construct_file_content(kvmap);
    if (ec->put(eid, buf) != extent_protocol::OK) {
        ret = ydb_protocol::RPCERR;
        goto release;
    }

release:
    lc->release(EID_MAX);
	return ret;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, std::string key, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    lc->acquire(EID_MAX);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        lc->acquire(eid);
        lock_set[id].insert(eid);
    }

    if (ec->get(eid, buf) != extent_protocol::OK) {
        ret = ydb_protocol::RPCERR;
        goto release;
    }

    kvmap = parse_file_content(buf);
    if (!log[id].count(key))
        log[id][key] = kvmap[key];
    kvmap.erase(key);

    buf = construct_file_content(kvmap);
    if (ec->put(eid, buf) != extent_protocol::OK) {
        ret = ydb_protocol::RPCERR;
        goto release;
    }

release:
    lc->release(EID_MAX);
	return ret;
}
