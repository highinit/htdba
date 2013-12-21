/* 
 * File:   htKeyScanner.h
 * Author: phrk
 *
 * Created on December 14, 2013, 9:42 PM
 */

#ifndef HTKEYSCANNER_H
#define	HTKEYSCANNER_H

#include "htDba.h"
#include "htConnPool.h"
#include <queue>

class htKeyScanner
{
	htConnPoolPtr m_conn_pool;
	std::string m_table;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	Hypertable::ThriftGen::Namespace m_ns;
	
	
	std::queue<std::string> buffer;
	
	void loadMore(htConnPool::htSession sess);
	void reset(htConnPool::htSession sess);
public:
	
	htKeyScanner(htConnPoolPtr conn_pool,
				std::string ns,
				std::string table,
				const KeyRange range = KeyRange::getEmptyRange());
	~htKeyScanner();
	
	std::string getNextKey();
	
	void reset(const KeyRange &range);
	void reset();
	bool end();
};

typedef boost::shared_ptr<htKeyScanner> htKeyScannerPtr;

#endif	/* HTKEYSCANNER_H */

