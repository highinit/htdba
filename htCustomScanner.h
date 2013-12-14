/* 
 * File:   htCustomScanner.h
 * Author: phrk
 *
 * Created on December 14, 2013, 6:29 PM
 */

#ifndef HTCUSTOMSCANNER_H
#define	HTCUSTOMSCANNER_H

#include "htDba.h"
#include <set>

class htCustomScanner
{	
	ThriftClientPtr m_client;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	
	std::set<std::string> m_colls;
	
	std::queue<htCell> cell_buffer;
	std::queue<htLine> line_buffer;
	
	size_t m_lines_in_buffer;
	
	void loadMore();
	void parseMore();
	
public:
	htCustomScanner(ThriftClientPtr client,
				std::string ns,
				std::string table,
				std::set<std::string> colls,
				size_t lines_in_buffer = 10);
	~htCustomScanner();
	
	htLine getNextLine();
	
	bool end();
};

typedef boost::shared_ptr<htCustomScanner> htCustomScannerPtr;

#endif	/* HTCUSTOMSCANNER_H */

