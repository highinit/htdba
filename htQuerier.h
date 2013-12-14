/* 
 * File:   htQuerier.h
 * Author: phrk
 *
 * Created on December 13, 2013, 7:44 PM
 */

#ifndef HTQUERIER_H
#define	HTQUERIER_H

#include "htDba.h"

class htQuerier
{
	ThriftClientPtr m_client;
	Hypertable::ThriftGen::Namespace m_ns;
	std::string m_table;

public:
	
	enum Ex
	{
		NO_CELLS,
		BROKEN_RESULT
	};
	
	htQuerier(ThriftClientPtr client,
				std::string ns,
				std::string table);
	
	std::string getColl(std::string key, std::string coll);
	
	~htQuerier();
};

typedef boost::shared_ptr<htQuerier> htQuerierPtr;

#endif	/* HTQUERIER_H */

