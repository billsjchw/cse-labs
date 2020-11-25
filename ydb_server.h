#ifndef ydb_server_h
#define ydb_server_h

#include <string>
#include <map>
#include <vector>
#include <set>
#include <utility>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "extent_protocol.h"


class ydb_server {
protected:
	extent_client *ec;
	lock_client *lc;
    static const extent_protocol::extentid_t EID_MIN = 2;
    static const extent_protocol::extentid_t EID_MAX = 1024;
    static const std::size_t KV_LEN_MAX = 32;
public:
	ydb_server(std::string, std::string);
	virtual ~ydb_server();
	virtual ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	virtual ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	virtual ydb_protocol::status get(ydb_protocol::transaction_id, std::string, std::string &);
	virtual ydb_protocol::status set(ydb_protocol::transaction_id, std::string, std::string, int &);
	virtual ydb_protocol::status del(ydb_protocol::transaction_id, std::string, int &);
protected:
    static extent_protocol::extentid_t hash(const std::string &);
    static std::string construct_file_content(const std::map<std::string, std::string> &);
    static std::map<std::string, std::string> parse_file_content(const std::string &);
};

#endif

