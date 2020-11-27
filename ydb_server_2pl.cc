#include <map>
#include <set>
#include <string>
#include "ydb_server_2pl.h"
#include "extent_client.h"
#include "extent_protocol.h"

//#define DEBUG 1

ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst), next_id(1) {
}

ydb_server_2pl::~ydb_server_2pl() {
}

ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	ydb_protocol::status ret = ydb_protocol::OK;
    
    lc->acquire(EID_MAX);

    out_id = next_id++;

    lc->acquire(EID_MAX + out_id);

    active_ids.insert(out_id);

    lc->release(EID_MAX);
    lc->release(EID_MAX + out_id);
    return ret;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	ydb_protocol::status ret = ydb_protocol::OK;
    
    if (id <= 0)
        return ydb_protocol::TRANSIDINV;

    lc->acquire(EID_MAX);
    lc->acquire(EID_MAX + id);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    clear(id);

release:
    lc->release(EID_MAX);
    lc->release(EID_MAX + id);
	return ret;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
    ydb_protocol::status ret = ydb_protocol::OK;
    
    if (id <= 0)
        return ydb_protocol::TRANSIDINV;

    lc->acquire(EID_MAX);
    lc->acquire(EID_MAX + id);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    rollback(id);
    clear(id);

release:
    lc->release(EID_MAX);
    lc->release(EID_MAX + id);
	return ret;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, std::string key, std::string &out_value) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    if (id <= 0)
        return ydb_protocol::TRANSIDINV;

    lc->acquire(EID_MAX);
    lc->acquire(EID_MAX + id);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        acquiring[id] = eid;
        if (detect_deadlock()) {
            rollback(id);
            clear(id);
            ret = ydb_protocol::ABORT;
            goto release;
        }
        lc->release(EID_MAX);
        lc->acquire(eid);
        lc->acquire(EID_MAX);
        acquiring.erase(id);
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
    lc->release(EID_MAX + id);
	return ret;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, std::string key, std::string value, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    if (id <= 0)
        return ydb_protocol::TRANSIDINV;

    lc->acquire(EID_MAX);
    lc->acquire(EID_MAX + id);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        acquiring[id] = eid;
        if (detect_deadlock()) {
            rollback(id);
            clear(id);
            ret = ydb_protocol::ABORT;
            goto release;
        }
        lc->release(EID_MAX);
        lc->acquire(eid);
        lc->acquire(EID_MAX);
        acquiring.erase(id);
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
    while (ec->put(eid, buf) != extent_protocol::OK);

release:
    lc->release(EID_MAX);
    lc->release(EID_MAX + id);
	return ret;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, std::string key, int &) {
	std::string buf;
    extent_protocol::extentid_t eid = hash(key);
    std::map<std::string, std::string> kvmap;
    ydb_protocol::status ret = ydb_protocol::OK;

    if (id <= 0)
        return ydb_protocol::TRANSIDINV;

    lc->acquire(EID_MAX);
    lc->acquire(EID_MAX + id);

    if (!active_ids.count(id)) {
        ret = ydb_protocol::TRANSIDINV;
        goto release;
    }

    if (!lock_set[id].count(eid)) {
        acquiring[id] = eid;
        if (detect_deadlock()) {
            rollback(id);
            clear(id);
            ret = ydb_protocol::ABORT;
            goto release;
        }
        lc->release(EID_MAX);
        lc->acquire(eid);
        lc->acquire(EID_MAX);
        acquiring.erase(id);
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
    while (ec->put(eid, buf) != extent_protocol::OK);

release:
    lc->release(EID_MAX);
    lc->release(EID_MAX + id);
	return ret;
}

bool ydb_server_2pl::detect_deadlock() {
    std::set<ydb_protocol::transaction_id> unfinished_ids;
    std::set<extent_protocol::extentid_t> unavailable_set;

    for (ydb_protocol::transaction_id id : active_ids)
        if (acquiring.count(id)) {
            unfinished_ids.insert(id);
            for (extent_protocol::extentid_t eid : lock_set[id])
                unavailable_set.insert(eid);
        }

    while (true) {
        ydb_protocol::transaction_id selected_id = 0;
        
        for (ydb_protocol::transaction_id id : unfinished_ids)
            if (!unavailable_set.count(acquiring[id])) {
                selected_id = id;
                break;
            }
        if (selected_id == 0)
            break;

        for (extent_protocol::extentid_t eid : lock_set[selected_id])
            unavailable_set.erase(eid);
        unfinished_ids.erase(selected_id);
    }

    return !unfinished_ids.empty();
}

void ydb_server_2pl::rollback(ydb_protocol::transaction_id id) {
    for (const auto &kv : log[id]) {
        std::string buf;
        extent_protocol::extentid_t eid = hash(kv.first);
        std::map<std::string, std::string> kvmap;

        while (ec->get(eid, buf) != extent_protocol::OK);

        kvmap = parse_file_content(buf);
        kvmap[kv.first] = kv.second;

        buf = construct_file_content(kvmap);
        while (ec->put(eid, buf) != extent_protocol::OK);
    }
}

void ydb_server_2pl::clear(ydb_protocol::transaction_id id) {
    for (extent_protocol::extentid_t eid : lock_set[id])
        lc->release(eid);

    active_ids.erase(id);
    lock_set.erase(id);
    log.erase(id);
    acquiring.erase(id);
}
