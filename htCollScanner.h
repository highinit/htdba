#ifndef PAGES_DBA_H
#define PAGES_DBA_H

#include "htDba.h"
#include "htConnPool.h"
#include <queue>
#include "../tetramorph/threadpool/threadpool.h"

class htCollScanner
{
	htConnPoolPtr m_conn_pool;
	Hypertable::ThriftGen::Namespace m_ns;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	
	std::string m_ns_name;
	
	std::string m_coll;
	std::string m_table;
	
	std::queue<KeyValue> buffer;
	
	void loadMore(htConnPool::htSession &sess);
	void reset(htConnPool::htSession &sess);
public:
	htCollScanner(htConnPoolPtr conn_pool,
				const std::string &_ns,
				const std::string &_table,
				const std::string &_coll,
				const KeyRange &_range = KeyRange::getEmptyRange());
	
	htCollScanner(const htCollScanner &a);
	htCollScanner(const htCollScanner* a);
	
	~htCollScanner();
	
	KeyValue getNextCell();
	void reset();
	void reset(const KeyRange &_range);
	
	bool end();
};

typedef boost::shared_ptr<htCollScanner> htCollScannerPtr;

class htCollScannerConc : public htCollScanner
{
	hLock lock;
public:
	
	KeyValue getNextCell();
	bool end();
};

typedef boost::shared_ptr<htCollScannerConc> htCollScannerConcPtr;

#endif