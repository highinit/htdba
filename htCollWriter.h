/* 
 * File:   htCollWriter.h
 * Author: phrk
 *
 * Created on December 11, 2013, 7:56 PM
 */

#ifndef HTCOLLWRITER_H
#define	HTCOLLWRITER_H

#include "htDba.h"
#include "htConnPool.h"
#include "hiaux/threads/tasklauncher.h"

class htCollWriterConc
{	
	htConnPoolPtr m_conn_pool;
	Hypertable::ThriftGen::Namespace m_ns;
	std::string m_table;
	
	bool m_stopping;
	bool m_stoped;
	
public:
	
	htCollWriterConc(htConnPoolPtr conn_pool,
				std::string ns,
				std::string table);
	
	~htCollWriterConc();
	TaskLauncher::TaskRet checkFlush();
	
	void insertAsync(KeyValue cell, std::string coll);
	void insertSync(const KeyValue &_cell, const std::string &_coll);
	void removeSync(std::string key, std::string coll);
};

typedef boost::shared_ptr<htCollWriterConc> htCollWriterConcPtr;

#endif	/* HTCOLLWRITER_H */

