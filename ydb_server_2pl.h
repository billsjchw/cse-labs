#ifndef ydb_server_2pl_h
#define ydb_server_2pl_h

#include <string>
#include <map>
#include <set>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"

class ydb_server_2pl: public ydb_server {
private:
    ydb_protocol::transaction_id next_id;
    std::set<ydb_protocol::transaction_id> active_ids;
    std::map<ydb_protocol::transaction_id, std::set<extent_protocol::extentid_t>> lock_set;
    std::map<ydb_protocol::transaction_id, std::map<std::string, std::string>> log;
    std::map<ydb_protocol::transaction_id, extent_protocol::extentid_t> acquiring;
public:
	ydb_server_2pl(std::string, std::string);
	~ydb_server_2pl();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, std::string, std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, std::string, int &);
private:
    bool detect_deadlock();
    void rollback(ydb_protocol::transaction_id id);
    void clear(ydb_protocol::transaction_id id);
};

#endif
