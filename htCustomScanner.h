/* 
 * File:   htCustomScanner.h
 * Author: phrk
 *
 * Created on December 14, 2013, 6:29 PM
 */

#ifndef HTCUSTOMSCANNER_H
#define	HTCUSTOMSCANNER_H

#include "htDba.h"
#include "htConnPool.h"
#include <set>

class htCustomScanner
{	
	htConnPoolPtr m_conn_pool;
	
	Hypertable::ThriftGen::Namespace m_ns;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	
	std::string m_sns;
	std::string m_table;
	std::set<std::string> m_colls;
	
	std::queue<htCell> cell_buffer;
	std::queue<htLine> line_buffer;
	
	size_t m_lines_in_buffer;
	
	void loadMore(htConnPool::htSession &_sess);
	void parseMore();
	
public:
	htCustomScanner(htConnPoolPtr _conn_pool,
				const std::string &_ns,
				const std::string &_table,
				const std::set<std::string> &_colls,
				size_t lines_in_buffer = 10);
	~htCustomScanner();
		
	void reset();
	void reset(const KeyRange &_range);
	htLine& getNextLine(htLine &line);
	
	bool end();
};

typedef boost::shared_ptr<htCustomScanner> htCustomScannerPtr;

#endif	/* HTCUSTOMSCANNER_H */

