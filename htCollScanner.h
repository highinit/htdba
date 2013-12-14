#ifndef PAGES_DBA_H
#define PAGES_DBA_H

#include "htDba.h"
#include <queue>
#include "../tetramorph/threadpool/threadpool.h"

class htCollScanner
{
	ThriftClientPtr m_client;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	
	std::string m_coll;
	
	std::queue<KeyValue> buffer;
	
	bool m_scanner_end;
	
	void loadMore();
	
public:
	htCollScanner(ThriftClientPtr client,
				std::string ns,
				std::string table,
				std::string coll);
	~htCollScanner();
	
	KeyValue getNextCell();
	
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