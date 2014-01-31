/* 
 * File:   htQuerier.h
 * Author: phrk
 *
 * Created on December 13, 2013, 7:44 PM
 */

#ifndef HTQUERIER_H
#define	HTQUERIER_H

#include "htDba.h"
#include "htConnPool.h"

class htQuerier
{
	htConnPoolPtr m_conn_pool;
	Hypertable::ThriftGen::Namespace m_ns;
	std::string m_table;

public:
	
	enum Ex
	{
		NO_CELLS,
		BROKEN_RESULT
	};
	
	htQuerier(htConnPoolPtr conn_pool,
				const std::string &_ns,
				const std::string &_table);
	
	bool getColl(const std::string &_key,
				const std::string &_coll,
				std::string &value);
	//void getKeys(const KeyRange &range, std::vector<std::string> &keys);
	
	~htQuerier();
};

typedef boost::shared_ptr<htQuerier> htQuerierPtr;

#endif	/* HTQUERIER_H */

