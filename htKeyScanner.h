/* 
 * File:   htKeyScanner.h
 * Author: phrk
 *
 * Created on December 14, 2013, 9:42 PM
 */

#ifndef HTKEYSCANNER_H
#define	HTKEYSCANNER_H

#include "htDba.h"
#include <queue>

class htKeyScanner
{
	ThriftClientPtr m_client;
	Hypertable::ThriftGen::ScanSpec m_ss;
	Hypertable::ThriftGen::Scanner m_s;
	
	std::queue<std::string> buffer;
	
	void loadMore();
	
public:
	
	htKeyScanner(ThriftClientPtr client,
				std::string ns,
				std::string table);
	~htKeyScanner();
	
	std::string getNextKey();
	bool end();
};

#endif	/* HTKEYSCANNER_H */

